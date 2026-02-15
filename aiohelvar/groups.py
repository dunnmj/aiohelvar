from aiohelvar.lib import Subscribable
from aiohelvar.static import DEFAULT_FADE_TIME
from aiohelvar.parser.address import HelvarAddress, SceneAddress
import asyncio
from aiohelvar.parser.command_parameter import CommandParameter, CommandParameterType
from .parser.command_type import CommandType, MessageType
from .parser.command import Command

import logging

_LOGGER = logging.getLogger(__name__)


def blockscene_to_block_and_scene(block_scene: int):
    scene = block_scene % 16
    block = ((block_scene - scene) / 16) + 1
    return block, scene + 1


class Group(Subscribable):
    def __init__(self, group_id: int, name=None):
        super(Group, self).__init__()
        self.group_id: int = group_id
        self.name = name
        self.devices = []
        self.last_scene_address = None

    def __str__(self):
        return f"Group {self.group_id}: {self.name}. Has {len(self.devices)} devices."

    def __hash__(self) -> int:
        return hash(self.group_id)

    def __eq__(self, o: object) -> bool:
        return self.group_id == o.group_id

    def get_last_scene_address(self):
        return self.last_scene_address

    def get_levels_for_scene(self, scene_address):
        pass


class Groups:
    def __init__(self, router):
        self.router = router
        self.groups = {}

    def register_group(self, group: Group):
        self.groups[int(group.group_id)] = group

    def update_group_name(self, group_id: int, name):
        self.groups[int(group_id)].name = name

    def update_group_device_members(self, group_id: int, addresses):
        self.groups[int(group_id)].devices = addresses

    def unregister_subscription(self, group_id, func):
        group = self.groups.get(group_id)

        if group:
            group.remove_subscriber(func)
            return True
        return False

    def register_subscription(self, group_id: int, func):
        group = self.groups.get(int(group_id))

        if group:
            group.add_subscriber(func)
            return True
        return False

    def get_scenes_for_group(self, group_id, only_named=True):
        return self.router.scenes.get_scenes_for_group(group_id, only_named)

    async def force_update_groups(self):
        """Force subscription updates for all groups."""
        for group in self.groups.values():
            await group.update_subscribers()

    async def handle_scene_callback(self, scene_address: SceneAddress, fade_time):
        if scene_address.group not in self.groups.keys():
            _LOGGER.info(
                f"Scene {scene_address} not in any known group. Looking for {scene_address.group} in {self.groups.keys()}. Ignoring."
            )
            return

        group = self.groups.get(scene_address.group)
        if not group:
            _LOGGER.error(
                f"Group {scene_address.group} not found for scene {scene_address}"
            )
            return
        group.last_scene_address = scene_address

        _LOGGER.info(
            f"Updating devices in group {group.name} to scene {scene_address}..."
        )
        for device_address in self.groups[scene_address.group].devices:
            device = self.router.devices.devices.get(device_address)
            if device is None:
                _LOGGER.warning(
                    f"Can't find device {device_address} registered in group {scene_address.group}."
                )
                continue
            await device.set_scene_level(scene_address)

        await group.update_subscribers()

        _LOGGER.info(f"Updated devices in scene {scene_address}.")

    async def handle_direct_level_group_callback(self, group_id: int, level: float):
        """Handle a Direct Level Group (command 13) echo.

        Updates member device load levels and notifies group subscribers
        so individual HelvarLight entities stay in sync.
        """
        group = self.groups.get(int(group_id))
        if not group:
            _LOGGER.debug(
                "Received direct level for unknown group %s, ignoring", group_id
            )
            return

        _LOGGER.info("Updating devices in group %s to level %s", group.name, level)
        for device_address in group.devices:
            device = self.router.devices.devices.get(device_address)
            if device is None:
                continue
            await device._set_level(level)
            await device.update_subscribers()

        await group.update_subscribers()

    async def set_scene(self, scene_address: SceneAddress, fade_time=DEFAULT_FADE_TIME):
        """Set the scene with the router.

        We'll get a scene change callback from the router that we'll use to
        update device state, so no need to call one here.
        """
        await self.router.send_command(
            Command(
                CommandType.RECALL_SCENE,
                [
                    CommandParameter(CommandParameterType.GROUP, scene_address.group),
                    CommandParameter(CommandParameterType.BLOCK, scene_address.block),
                    CommandParameter(CommandParameterType.SCENE, scene_address.scene),
                    CommandParameter(CommandParameterType.FADE_TIME, fade_time),
                ],
            )
        )

    async def set_group_level(self, group_id: int, level: str, fade_time=100):
        """Set the load level for all devices in a group.

        Uses Direct Level Group (command 13):
        >V:1,C:13,G:<group>,L:<level>,F:<fade>#

        Args:
            group_id: The Helvar group ID.
            level: Load level 0-100 as a string.
            fade_time: Fade time in centiseconds (100 = 1 second).
        """
        _LOGGER.info(
            "Setting group %s level to %s over %s cs", group_id, level, fade_time
        )

        await self.router._send_command_task(
            Command(
                CommandType.DIRECT_LEVEL_GROUP,
                [
                    CommandParameter(CommandParameterType.GROUP, str(group_id)),
                    CommandParameter(CommandParameterType.LEVEL, level),
                    CommandParameter(CommandParameterType.FADE_TIME, str(fade_time)),
                ],
            )
        )

        # Update member devices immediately so state is reflected
        await self.handle_direct_level_group_callback(group_id, float(level))

    async def set_group_colour_temperature(
        self, group_id: int, level: str, colour_temp: int, fade_time=100
    ):
        """Set the colour temperature for all devices in a group.

        Uses Direct Level Group (command 13) with M: (mireds) parameter:
        >V:1,C:13,G:<group>,L:<level>,F:<fade>,M:<mireds>#

        Args:
            group_id: The Helvar group ID.
            level: Load level 0-100 as a string.
            colour_temp: Colour temperature in mireds.
            fade_time: Fade time in centiseconds (100 = 1 second).
        """
        _LOGGER.info(
            "Setting group %s colour temperature to %s mireds at level %s",
            group_id,
            colour_temp,
            level,
        )

        await self.router._send_command_task(
            Command(
                CommandType.DIRECT_LEVEL_GROUP,
                [
                    CommandParameter(CommandParameterType.GROUP, str(group_id)),
                    CommandParameter(CommandParameterType.LEVEL, level),
                    CommandParameter(CommandParameterType.FADE_TIME, str(fade_time)),
                    CommandParameter(CommandParameterType.MIREDS, str(colour_temp)),
                ],
            )
        )

        await self.handle_direct_level_group_callback(group_id, float(level))

    async def set_group_xy_color(
        self, group_id: int, level: str, x: float, y: float, fade_time=100
    ):
        """Set the CX/CY colour for all devices in a group.

        Uses Direct Level Group (command 13) with CX: and CY: parameters:
        >V:1,C:13,G:<group>,L:<level>,F:<fade>,CX:<cx>,CY:<cy>#

        Args:
            group_id: The Helvar group ID.
            level: Load level 0-100 as a string.
            x: CIE x coordinate (0.0-1.0).
            y: CIE y coordinate (0.0-1.0).
            fade_time: Fade time in centiseconds (100 = 1 second).
        """
        cx = int(x * 65535)
        cy = int(y * 65535)

        _LOGGER.info(
            "Setting group %s XY colour to (%s, %s) at level %s",
            group_id,
            cx,
            cy,
            level,
        )

        await self.router._send_command_task(
            Command(
                CommandType.DIRECT_LEVEL_GROUP,
                [
                    CommandParameter(CommandParameterType.GROUP, str(group_id)),
                    CommandParameter(CommandParameterType.LEVEL, level),
                    CommandParameter(CommandParameterType.FADE_TIME, str(fade_time)),
                    CommandParameter(CommandParameterType.COLOUR_X, str(cx)),
                    CommandParameter(CommandParameterType.COLOUR_Y, str(cy)),
                ],
            )
        )

        await self.handle_direct_level_group_callback(group_id, float(level))


async def get_groups(router):
    response = await router._send_command_task(Command(CommandType.QUERY_GROUPS))

    # We expect a comma separated list of group ids.
    async def update_name(router, group_id):
        response = await router._send_command_task(
            Command(
                CommandType.QUERY_GROUP_DESCRIPTION,
                [CommandParameter(CommandParameterType.GROUP, group_id)],
            )
        )
        router.groups.update_group_name(group_id, response.result)

    async def update_group_devices(router, group_id):
        response = await router._send_command_task(
            Command(
                CommandType.QUERY_GROUP,
                [CommandParameter(CommandParameterType.GROUP, group_id)],
            )
        )

        if response.result is not None:
            members = [member.strip("@") for member in response.result.split(",")]
            _LOGGER.debug(f"members is '{members}'")

            addresses = [HelvarAddress(*member.split(".")) for member in members]
            _LOGGER.debug(f"addresses is '{addresses}'")

            router.groups.update_group_device_members(group_id, addresses)

    async def update_group_last_scene(router, group_id):
        response = await router._send_command_task(
            Command(
                CommandType.QUERY_LAST_SCENE_IN_GROUP,
                [CommandParameter(CommandParameterType.GROUP, group_id)],
            )
        )

        if response.command_message_type != MessageType.REPLY:
            if response.command_message_type != MessageType.ERROR:
                _LOGGER.error(f"Error reply to command: {response}")
                return
            _LOGGER.error(f"Unexpected reply to command: {response}")

        try:
            block_scene = int(response.result)
        except (ValueError, TypeError):
            _LOGGER.error(f"Invalid block_scene value: {response.result}")
            return
        scene_address = SceneAddress(
            group_id, *blockscene_to_block_and_scene(block_scene)
        )
        await router.groups.handle_scene_callback(scene_address, 10)

    if not response.result:
        _LOGGER.debug(
            "Response to QUERY_GROUPS command was empty. Assuming no groups defined."
        )
        return

    # TODO: Validate input - Regex for comma separated ints would do

    try:
        group_ids = response.result.split(",")
        groups = []
        for group_id in group_ids:
            group_id = group_id.strip()
            if group_id:  # Skip empty strings
                try:
                    # Validate that group_id is numeric
                    int(group_id)
                    groups.append(Group(group_id))
                except ValueError:
                    _LOGGER.warning(f"Invalid group ID: {group_id}")
    except AttributeError:
        _LOGGER.error("Response result is not a string - cannot parse groups")
        return

    for group in groups:
        router.groups.register_group(group)

    # Await all metadata tasks so that names, devices, and scenes are
    # fully populated before callers create entities.
    await asyncio.gather(
        *[
            task
            for group in groups
            for task in (
                update_name(router, group.group_id),
                update_group_devices(router, group.group_id),
                update_group_last_scene(router, group.group_id),
            )
        ]
    )
