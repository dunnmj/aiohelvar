from aiohelvar.parser.command_parameter import CommandParameterType
from .devices import Devices, get_devices, receive_and_register_devices
from .groups import Groups, get_groups
from .scenes import Scenes, get_scenes
from .parser.parser import CommandParser
from .parser.command_type import (
    COMMAND_TYPES_DONT_LISTEN_FOR_RESPONSE,
    CommandType,
    MessageType,
)
from .parser.command import Command
from .parser.address import HelvarAddress
from .exceptions import CommandResponseTimeout, ParserError
import asyncio
import datetime
import logging
import ipaddress

_LOGGER = logging.getLogger(__name__)


COMMAND_TERMINATOR = b"#"

# Some commands take a long time to process, and if the router has a significant queue, we
# can be waiting some time. Setting this to a somewhat absurd 30 seconds.
COMMAND_RESPONSE_TIMEOUT = 30

KEEP_ALIVE_PERIOD = 120


class Router:
    """Control a Helvar Router."""

    def __init__(self, host, port, cluster_id=0, router_id=1, use_specified_ids=False):
        self.host = host
        self.port = port

        # Check if we should use specified IDs or extract from IP address
        if use_specified_ids:
            # Use the provided cluster_id and router_id values
            _LOGGER.debug(
                f"Using specified IDs: cluster_id={cluster_id}, router_id={router_id}"
            )
            self.cluster_id = cluster_id
            self.router_id = router_id
        else:
            # Check if host is a valid IP address and extract cluster_id and router_id
            try:
                ip = ipaddress.ip_address(host)
                if isinstance(ip, ipaddress.IPv4Address):
                    octets = str(ip).split(".")
                    self.cluster_id = int(octets[2])  # 3rd octet
                    self.router_id = int(octets[3])  # 4th octet
                    _LOGGER.debug(
                        f"Extracted IDs from IPv4 address {host}: cluster_id={self.cluster_id}, router_id={self.router_id}"
                    )
                else:
                    # For IPv6 or if we can't parse octets, use provided values
                    _LOGGER.debug(
                        f"IPv6 address {host} detected, using provided values: cluster_id={cluster_id}, router_id={router_id}"
                    )
                    self.cluster_id = cluster_id
                    self.router_id = router_id
            except ValueError:
                # Not a valid IP address, use provided values
                _LOGGER.debug(
                    f"Invalid IP address '{host}', using provided values: cluster_id={cluster_id}, router_id={router_id}"
                )
                self.cluster_id = cluster_id
                self.router_id = router_id

        self.config = None

        self.groups = Groups(self)

        self.devices = Devices(self)

        self.lights = None
        self.scenes = Scenes(self)
        self.sensors = None

        self.commands_to_send = asyncio.Queue()

        self.commands_received = []
        self.command_received = asyncio.Condition()

        self.connected = False

        self.workgroup_name = None

    @property
    def id(self):
        """Return the ID of the router."""
        if self.config is not None:
            return self.config.routerid

        return self._router_id

    async def connect(self):
        _LOGGER.debug("Connecting...")

        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=10
            )
        except ConnectionError as e:
            _LOGGER.error(
                f"Connection error while connecting to router {self.host}:{self.port} - ",
                e,
            )
            raise
        except asyncio.TimeoutError as e:
            _LOGGER.error(
                f"Timeout while connecting to router {self.host}:{self.port} - ",
                e,
            )
            raise
        self.connected = True
        self._stream_reader_task = asyncio.create_task(
            self._stream_reader(self._reader)
        )
        self._stream_writer_task = asyncio.create_task(
            self._stream_writer(self._reader, self._writer)
        )

        # Read the workgroup name:
        response = await self._send_command_task(
            Command(CommandType.QUERY_WORKGROUP_NAME)
        )
        self.workgroup_name = response.result

        # Kick off the keepalive task
        self._keep_alive_task = asyncio.create_task(self._keep_alive())

    async def reconnect(self):
        await self.disconnect()
        await self.connect()

    async def disconnect(self):
        _LOGGER.info("Disconnecting...")
        tasks = [
            self._stream_reader_task,
            self._stream_writer_task,
            self._keep_alive_task,
        ]

        for task in tasks:
            if task is not None:
                task.cancel()

        self._writer.close()
        await self._writer.wait_closed()
        self.connected = False
        _LOGGER.info("Disconnected.")

    async def _keep_alive(self):
        """Keep the TCP connection alive. This'll also clean up any stale command futures."""

        def _keep_alive_callback(task):
            if task.exception():
                _LOGGER.warn(
                    f"Keep alive encountered an exception: {task.exception()}."
                )
                if isinstance(task.exception(), CommandResponseTimeout):
                    # Timeout - reconnect.
                    _LOGGER.warn("Keepalive didn't - reconnecting...")
                    asyncio.create_task(self.reconnect())
                    return
                else:
                    raise (task.exception())
            _LOGGER.debug("Keepalive kept the router TCP connection alive.")

        while True:
            await asyncio.sleep(KEEP_ALIVE_PERIOD)
            keepalive = await self.send_command(Command(CommandType.QUERY_ROUTER_TIME))

            keepalive.add_done_callback(_keep_alive_callback)

    async def _stream_reader(self, reader):
        _LOGGER.info("Connected.")
        parser = CommandParser()

        while True:
            line = await reader.readuntil(COMMAND_TERMINATOR)
            if line is not None:
                _LOGGER.debug(f"Received line: {line}")

                lines = line.split(b"$")
                if len(lines) > 1:
                    _LOGGER.debug(f"Split line by '$' into {len(lines)} lines")

                for splitline in lines:
                    _LOGGER.debug(f"Parsing line: {splitline}")
                    try:
                        command = parser.parse_command(splitline)
                    except ParserError as e:
                        _LOGGER.error(f"Exception handling line from router: {e}")
                    except Exception as e:
                        raise e
                    else:
                        _LOGGER.info(f"Received command: {command}")

                        if command.command_type == CommandType.RECALL_SCENE:
                            asyncio.create_task(self.handle_scene_recall(command))
                            continue

                        await self.command_received.acquire()
                        self.commands_received.append(command)
                        self.command_received.notify_all()
                        self.command_received.release()

    async def _stream_writer(self, reader, writer):
        while True:
            command_string = await self.commands_to_send.get()
            _LOGGER.info(f"Sending command '{command_string}'...")
            writer.write(command_string)
            # Small buffer. It's possible to overload a router.
            await asyncio.sleep(0.01)
            await writer.drain()
            self.commands_to_send.task_done()

    async def wait_for_pending_replies(self):
        while True:
            if len(self.command_received._waiters) == 0:
                return
            await asyncio.sleep(0.1)

    async def initialize(self, discover_cluster=False):
        # Attempt Connection
        if not self.connected:
            await self.connect()

        # Get Groups
        await self.get_groups()

        # Get Devices — either from the whole cluster or just this router
        if discover_cluster:
            await self.discover_devices_from_cluster()
        else:
            await self.get_devices()

        # Get Clusters
        # await self.get_clusters()

        # Get Scenes
        await self.get_scenes()

        # Update group scenes
        await self.groups.force_update_groups()

    async def get_groups(self):
        await get_groups(self)

    async def get_devices(self):
        await get_devices(self)

    async def get_scenes(self):
        await get_scenes(self, self.groups)

    async def query_cluster_routers_addresses(self):
        """Query the cluster for all router addresses.

        Sends command >V:2,C:108# and parses the comma-separated list of
        @cluster.router addresses. Returns a list of (cluster_id, router_id)
        tuples for all routers in the cluster.
        """
        response = await self._send_command_task(Command(CommandType.QUERY_ROUTERS))

        if not response or not response.result:
            _LOGGER.debug("No routers returned from QUERY_ROUTERS")
            return []

        routers = []
        for entry in response.result.split(","):
            ip = entry.strip().lstrip("@")
            routers.append(ip)

        _LOGGER.info("Discovered %d routers in the cluster", len(routers))
        return routers

    async def discover_devices_from_cluster(self):
        """Connect to each router in the cluster to discover devices with names.

        Each router can only return names for devices it directly controls.
        We connect to every router in the cluster, query their devices (with
        names), and register them all on this router's Devices store so that
        entities can be created from a single source of truth.
        """
        cluster_routers = await self.query_cluster_routers_addresses()

        if not cluster_routers:
            _LOGGER.info("No cluster routers found, using local devices only")
            await self.get_devices()
            return

        for router_ip in cluster_routers:
            if router_ip == self.host:
                # This is us — query devices directly
                _LOGGER.debug("Querying devices from local router %s", router_ip)
                await self.get_devices()
                continue

            _LOGGER.info(
                "Connecting to cluster router %s to discover devices",
                router_ip,
            )

            peer = Router(router_ip, self.port)

            try:
                await peer.connect()
                await peer.get_devices()
            except (ConnectionError, CommandResponseTimeout, OSError) as err:
                _LOGGER.warning(
                    "Could not connect to cluster router %s: %s",
                    router_ip,
                    err,
                )
                continue
            finally:
                if peer.connected:
                    try:
                        await peer.disconnect()
                    except Exception:  # noqa: BLE001
                        _LOGGER.debug("Error disconnecting from peer %s", router_ip)

            # Merge discovered devices into our device store
            for address, device in peer.devices.devices.items():
                if address not in self.devices.devices:
                    self.devices.register_device(device)
                    _LOGGER.debug(
                        "Registered device %s (%s) from router %s",
                        address,
                        device.name,
                        router_ip,
                    )
                elif device.name and not self.devices.devices[address].name:
                    # Peer had the name, we didn't
                    self.devices.devices[address].name = device.name
                    _LOGGER.debug(
                        "Updated name for device %s to '%s' from router %s",
                        address,
                        device.name,
                        router_ip,
                    )

    # async def get_clusters(self):
    #     response = await self.send_command(Command(CommandType.QUERY_ROUTERS))

    #     await response

    #     print(response.result())

    async def _send_command_task(self, command: Command):
        start_time = datetime.datetime.now()

        await self.send_string(str(command))

        def check_for_command_response():
            """Task that is scheduled after every command is sent. It checks for incoming messages
            from the router, looking for its reply.
            We match all command parameters, but we can't guarantee that identical requests don't steal
            eachothers replies."""

            for r_command in self.commands_received:
                if r_command.type_parameters_address == command.type_parameters_address:
                    # this is probably our response.
                    # We can safely remove ourselves from list as we stop iterating.

                    if r_command.command_message_type == MessageType.ERROR:
                        _LOGGER.error(
                            f"Request command {command} triggered an error back from the router: {r_command}."
                        )

                    self.commands_received.remove(r_command)
                    return r_command
            return None

        if command.command_type in COMMAND_TYPES_DONT_LISTEN_FOR_RESPONSE:
            return None

        response = check_for_command_response()

        if response:
            return response

        async with self.command_received:
            while response is None:
                if datetime.datetime.now() > (
                    start_time + datetime.timedelta(0, COMMAND_RESPONSE_TIMEOUT)
                ):
                    raise CommandResponseTimeout(command)

                await self.command_received.wait()

                response = check_for_command_response()
                if response:
                    break

        return response

    async def send_command(self, command: Command) -> asyncio.Task:
        """
        Send command, return a future that'll return when we get a response back.
        We don't have request identifiers, so we have to use basic FIFO and
        assume the router executes commands in the order it received them.
        """
        return asyncio.create_task(self._send_command_task(command))

    async def send_string(self, string: str):
        await self.commands_to_send.put(bytes(string, "utf-8"))

    async def handle_scene_recall(self, command: Command):
        """
        The only notifications we get on live changes in levels of devices is through scenes.
        """

        scene_address = command.get_scene_address()
        fade_time = command.get_param_value(CommandParameterType.FADE_TIME)

        await self.groups.handle_scene_callback(scene_address, fade_time)
