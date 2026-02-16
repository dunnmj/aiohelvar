from aiohelvar.parser.command_parameter import CommandParameter, CommandParameterType
from .devices import Device, Devices, get_devices, receive_and_register_devices
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
from copy import copy
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

        return self.router_id

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

    async def initialize(
        self,
        discover_cluster: bool = False,
        lights_only: bool = False,
    ) -> None:
        """Initialize the router.

        Args:
            discover_cluster: Query peer routers for device names.
            lights_only: Only discover devices and groups (minimal),
                         skip full group metadata and scenes.
        """
        if lights_only:
            # Query devices and groups minimally — no scenes or scene levels
            await self._get_devices_minimal()
            await self._get_groups_minimal()
        else:
            await get_groups(self)
            await get_devices(self)
            await get_scenes(self, self.groups)

        if discover_cluster:
            await self.discover_devices_from_cluster()

    async def _get_devices_minimal(self) -> None:
        """Query devices with only name and type — no state, levels, or scenes."""
        for subnet in range(1, 5):
            base_address = HelvarAddress(
                self.cluster_id, self.router_id, subnet,
            )
            try:
                response = await self._send_command_task(
                    Command(
                        CommandType.QUERY_DEVICE_TYPES_AND_ADDRESSES,
                        command_address=base_address,
                    )
                )
            except Exception:
                _LOGGER.debug("No devices found on subnet %s", subnet)
                continue

            if not response or not response.result:
                continue

            if "@" not in response.result:
                _LOGGER.debug(
                    "Not able to split, '%s' does not contain @",
                    response.result,
                )
                continue

            device_pairs = response.result.split(",")
            tasks = []
            for pair in device_pairs:
                parts = pair.split("@")
                if len(parts) != 2:
                    continue
                device_type = parts[0]
                # Build address from the base, setting the device octet
                address = copy(base_address)
                address.device = parts[1]
                device = Device(address, raw_type=device_type)
                self.devices.register_device(device)
                # Only query name and DALI device type
                tasks.append(self._update_device_name(device))
                tasks.append(self._update_device_type(device))

            await asyncio.gather(*tasks, return_exceptions=True)

    async def _update_device_name(self, device: Device) -> None:
        """Query only the device name."""
        try:
            response = await self._send_command_task(
                Command(
                    CommandType.QUERY_DEVICE_DESCRIPTION,
                    command_address=device.address,
                )
            )
        except Exception:
            _LOGGER.debug("Failed to query name for %s", device.address)
            return

        if response and response.result:
            device.name = response.result

    async def _update_device_type(self, device: Device) -> None:
        """Query only the DALI device type."""
        try:
            response = await self._send_command_task(
                Command(
                    CommandType.QUERY_DEVICE_TYPE,
                    command_address=device.address,
                )
            )
        except Exception:
            _LOGGER.debug("Failed to query type for %s", device.address)
            return

        if response and response.result:
            raw_val = int(response.result)
            if raw_val > 255:
                # Packed bytecode: byte[0]=protocol, byte[1]=DALI type
                device.device_type_id = (raw_val >> 8) & 0xFF
            else:
                device.device_type_id = raw_val

    async def _get_groups_minimal(self) -> None:
        """Query groups with only name and members — no scenes or last scene.

        Sends QUERY_GROUPS (165) to get group IDs, then gathers
        QUERY_GROUP_DESCRIPTION (105) and QUERY_GROUP (164) per group.
        """
        from .groups import Group

        try:
            response = await self._send_command_task(
                Command(CommandType.QUERY_GROUPS)
            )
        except Exception:
            _LOGGER.debug("Failed to query groups")
            return

        if not response or not response.result:
            _LOGGER.debug("No groups returned from QUERY_GROUPS")
            return

        # Parse comma-separated group IDs
        group_ids = []
        for gid in response.result.split(","):
            gid = gid.strip()
            if gid:
                try:
                    int(gid)
                    group_ids.append(gid)
                except ValueError:
                    _LOGGER.warning("Invalid group ID: %s", gid)

        # Register groups
        for gid in group_ids:
            self.groups.register_group(Group(gid))

        # Query name and members for each group in parallel
        async def update_name(group_id):
            try:
                resp = await self._send_command_task(
                    Command(
                        CommandType.QUERY_GROUP_DESCRIPTION,
                        [CommandParameter(CommandParameterType.GROUP, group_id)],
                    )
                )
            except Exception:
                _LOGGER.debug("Failed to query name for group %s", group_id)
                return
            if resp and resp.result:
                self.groups.update_group_name(group_id, resp.result)

        async def update_members(group_id):
            try:
                resp = await self._send_command_task(
                    Command(
                        CommandType.QUERY_GROUP,
                        [CommandParameter(CommandParameterType.GROUP, group_id)],
                    )
                )
            except Exception:
                _LOGGER.debug("Failed to query members for group %s", group_id)
                return
            if resp and resp.result:
                members = [m.strip("@") for m in resp.result.split(",")]
                addresses = [
                    HelvarAddress(*m.split(".")) for m in members
                ]
                self.groups.update_group_device_members(group_id, addresses)

        await asyncio.gather(
            *[
                task
                for gid in group_ids
                for task in (update_name(gid), update_members(gid))
            ],
            return_exceptions=True,
        )

    async def query_cluster_routers_addresses(self):
        """Query the cluster for all router addresses.

        Sends command >V:2,C:108# and parses the comma-separated list of
        @cluster.router addresses. Returns a list of "cluster.router" strings
        for all routers in the cluster (e.g. ["110.1", "110.2", ...]).
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

    async def discover_devices_from_cluster(self) -> None:
        """Discover devices from all routers in the cluster."""
        cluster_routers = await self.query_cluster_routers_addresses()

        for peer_ip in cluster_routers:
            # query_cluster_routers_addresses returns full IPs
            # (e.g. "10.86.110.2") stripped of the leading @

            if peer_ip == self.host:
                # Local router — devices already loaded
                continue

            _LOGGER.debug("Querying peer router at %s for device names", peer_ip)

            try:
                peer = Router(peer_ip, self.port)
                await peer.connect()
            except (ConnectionError, asyncio.TimeoutError):
                _LOGGER.warning(
                    "Could not connect to peer router at %s", peer_ip
                )
                continue

            try:
                # Discover devices on the peer router (minimal — names and types only)
                await peer._get_devices_minimal()

                # Merge peer devices into local device store
                for device in peer.devices.devices.values():
                    if device.address in self.devices.devices:
                        if device.name and not self.devices.devices[device.address].name:
                            self.devices.devices[device.address].name = device.name
                    else:
                        self.devices.register_device(device)
            except Exception:
                _LOGGER.debug(
                    "Failed to query devices from peer %s", peer_ip
                )
            finally:
                await peer.disconnect()

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
