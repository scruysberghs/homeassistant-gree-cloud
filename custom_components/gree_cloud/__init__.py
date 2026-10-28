"""The Gree Climate Cloud integration."""

from __future__ import annotations

import logging

from greeclimate_davo22.cloud_api import GreeCloudApi
from greeclimate_davo22.mqtt_client import GreeMqttClient

from homeassistant.const import CONF_PASSWORD, CONF_USERNAME, Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .const import CONF_SERVER, DOMAIN, GREE_MQTT_SERVERS
from .coordinator import (
    CloudDiscoveryService,
    GreeCloudConfigEntry,
    GreeCloudRuntimeData,
)

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.CLIMATE, Platform.SENSOR, Platform.SWITCH, Platform.WATER_HEATER]

# Substrings that identify an MQTT "not connected" error from paho / aiomqtt.
_MQTT_DISCONNECTED_MARKERS = ("code:4", "not currently connected", "not connected")


async def async_reconnect_mqtt(hass: HomeAssistant, entry: GreeCloudConfigEntry) -> bool:
    """Re-establish the MQTT connection after a broker disconnect.

    Returns True if the reconnect succeeded, False otherwise.
    Acquires the per-entry lock so that concurrent poll cycles don't each
    try to reconnect simultaneously.
    """
    runtime = entry.runtime_data
    lock = runtime.mqtt_reconnect_lock

    if lock.locked():
        # Another coroutine is already reconnecting — wait for it to finish,
        # then return (the client will already be fresh).
        async with lock:
            pass
        return runtime.mqtt_client.is_connected

    async with lock:
        _LOGGER.warning("MQTT disconnected — attempting to reconnect")

        old_client = runtime.mqtt_client
        mqtt_server = GREE_MQTT_SERVERS.get(entry.data[CONF_SERVER], "mqtt-eu.gree.com")

        try:
            # Re-login to get a fresh token (tokens can expire).
            credentials = await runtime.cloud_api.login()

            new_client = GreeMqttClient(
                credentials.user_id,
                credentials.token,
                server=mqtt_server,
            )
            await new_client.connect()
        except Exception as err:
            _LOGGER.error("MQTT reconnect failed during connect: %s", err)
            return False

        # Swap the client reference on every device and re-subscribe.
        for coordinator in runtime.coordinators:
            device = coordinator.device
            try:
                # Remove the old handler registered against the old client.
                old_client.remove_message_handler(device._handle_mqtt_message)
            except Exception:
                pass
            device._mqtt_client = new_client
            new_client.add_message_handler(device._handle_mqtt_message)
            try:
                # bind() re-subscribes to response/status/connect topics.
                await device.bind()
            except Exception as err:
                _LOGGER.warning(
                    "Failed to re-bind device %s after reconnect: %s",
                    device.device_info.name,
                    err,
                )

        runtime.mqtt_client = new_client

        # Best-effort cleanup of the old client.
        try:
            await old_client.disconnect()
        except Exception:
            pass

        _LOGGER.info("MQTT reconnect successful")
        return True


async def async_setup_entry(hass: HomeAssistant, entry: GreeCloudConfigEntry) -> bool:
    """Set up Gree Climate Cloud from a config entry."""
    _LOGGER.info("Setting up Gree Climate Cloud integration")

    api = None
    mqtt_client = None
    coordinators = []
    try:
        # Create Cloud API client
        api = GreeCloudApi.for_server(
            entry.data[CONF_SERVER],
            entry.data[CONF_USERNAME],
            entry.data[CONF_PASSWORD],
        )

        # Login to cloud
        _LOGGER.debug("Logging in to Gree Cloud")
        try:
            credentials = await api.login()
        except Exception as err:
            # The cloud rejected the credentials (e.g. a changed password).
            # Retrying in a loop would only hammer the account, so give up.
            if str(err).startswith("Login failed"):
                _LOGGER.error("Gree Cloud rejected the login: %s", err)
                await _close_quietly(api.close)
                return False
            raise

        # Create MQTT client
        _LOGGER.debug("Connecting to Gree MQTT broker")
        mqtt_server = GREE_MQTT_SERVERS.get(entry.data[CONF_SERVER], "mqtt-eu.gree.com")
        if entry.data[CONF_SERVER] not in GREE_MQTT_SERVERS:
            _LOGGER.warning(
                "Unknown server region '%s', falling back to Europe MQTT server",
                entry.data[CONF_SERVER],
            )
        mqtt_client = GreeMqttClient(credentials.user_id, credentials.token, server=mqtt_server)
        await mqtt_client.connect()

        # Store runtime data
        entry.runtime_data = GreeCloudRuntimeData(
            cloud_api=api,
            mqtt_client=mqtt_client,
            coordinators=[],
        )

        # Discover and setup devices
        discovery = CloudDiscoveryService(hass, entry, api)
        coordinators = await discovery.discover_devices(mqtt_client)
        entry.runtime_data.coordinators = coordinators

        _LOGGER.info("Successfully discovered %d cloud devices", len(coordinators))

        # Setup platforms
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

        return True

    except Exception as err:
        # Anything else (the MQTT broker refusing a fresh token, network or
        # cloud hiccups) is usually transient. Clean up and let Home Assistant
        # retry the setup instead of leaving the entry failed until someone
        # reloads it by hand.
        for coordinator in coordinators:
            await _close_quietly(coordinator.device.close)
        if mqtt_client is not None:
            await _close_quietly(mqtt_client.disconnect)
        if api is not None:
            await _close_quietly(api.close)
        raise ConfigEntryNotReady(f"Gree Cloud setup failed: {err}") from err


async def _close_quietly(close) -> None:
    """Await a close/disconnect coroutine function, ignoring any error."""
    try:
        await close()
    except Exception as err:
        _LOGGER.debug("Ignoring error during cleanup: %s", err)


async def async_unload_entry(hass: HomeAssistant, entry: GreeCloudConfigEntry) -> bool:
    """Unload a config entry."""
    _LOGGER.info("Unloading Gree Climate Cloud integration")

    # Unload platforms
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    if unload_ok:
        # Close all devices
        for coordinator in entry.runtime_data.coordinators:
            try:
                await coordinator.device.close()
            except Exception as err:
                _LOGGER.warning("Error closing device: %s", err)

        # Disconnect MQTT client
        try:
            await entry.runtime_data.mqtt_client.disconnect()
        except Exception as err:
            _LOGGER.warning("Error disconnecting MQTT client: %s", err)

        # Close API session
        try:
            await entry.runtime_data.cloud_api.close()
        except Exception as err:
            _LOGGER.warning("Error closing API session: %s", err)

    return unload_ok
