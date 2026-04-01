from pylitterbot.enums import LitterBoxStatus

"""
Litter Robot API Events, from https://github.com/natekspencer/pylitterbot/blob/main/pylitterbot/enums.py

"""

BONNET_REMOVED = "Bonnet Removed"
CAT_DETECTED = "Cat Detected"
CAT_SENSOR_FAULT = "Cat Sensor Fault"
CAT_SENSOR_INTERRUPTED = "Cat Sensor Interrupted"
CAT_SENSOR_TIMING = "Cat Sensor Timing"
CLEAN_CYCLE_COMPLETE = "Clean Cycle Complete"
CLEAN_CYCLE_IN_PROGRESS = "Clean Cycle In Progress"
CLEAN_CYCLES = "Clean Cycles"
DRAWER_FULL = "Drawer Full"
DRAWER_FULL_1 = "Drawer Full 1"
DRAWER_FULL_2 = "Drawer Full 2"
DUMP_HOME_POSITION_FAULT = "Dump Home Position Fault"
DUMP_POSITION_FAULT = "Dump Position Fault"
EMPTY_CYCLE = "Empty Cycle"
HOME_POSITION_FAULT = "Home Position Fault"
OFF = "Off"
OFFLINE = "Offline"
OVER_TORQUE_FAULT = "Over Torque Fault"
PAUSED = "Paused"
PET_WEIGHT_RECORDED = "Pet Weight Recorded"
PINCH_DETECT = "Pinch Detect"
POWER_DOWN = "Power Down"
POWER_UP = "Power Up"
READY = "Ready"
STARTUP_CAT_SENSOR_FAULT = "Startup Cat Sensor Fault"
STARTUP_DRAWER_FULL = "Startup Drawer Full"
STARTUP_PINCH_DETECT = "Startup Pinch Detect"
UNKNOWN = "Unknown"

STATUS_TO_EVENT = {
    LitterBoxStatus.BONNET_REMOVED: BONNET_REMOVED,
    LitterBoxStatus.CAT_DETECTED: CAT_DETECTED,
    LitterBoxStatus.CAT_SENSOR_FAULT: CAT_SENSOR_FAULT,
    LitterBoxStatus.CAT_SENSOR_INTERRUPTED: CAT_SENSOR_INTERRUPTED,
    LitterBoxStatus.CAT_SENSOR_TIMING: CAT_SENSOR_TIMING,
    LitterBoxStatus.CLEAN_CYCLE_COMPLETE: CLEAN_CYCLE_COMPLETE,
    LitterBoxStatus.CLEAN_CYCLE: CLEAN_CYCLE_IN_PROGRESS,
    LitterBoxStatus.DRAWER_FULL: DRAWER_FULL,
    LitterBoxStatus.DRAWER_FULL_1: DRAWER_FULL_1,
    LitterBoxStatus.DRAWER_FULL_2: DRAWER_FULL_2,
    LitterBoxStatus.DUMP_HOME_POSITION_FAULT: DUMP_HOME_POSITION_FAULT,
    LitterBoxStatus.DUMP_POSITION_FAULT: DUMP_POSITION_FAULT,
    LitterBoxStatus.EMPTY_CYCLE: EMPTY_CYCLE,
    LitterBoxStatus.HOME_POSITION_FAULT: HOME_POSITION_FAULT,
    LitterBoxStatus.OFF: OFF,
    LitterBoxStatus.OFFLINE: OFFLINE,
    LitterBoxStatus.OVER_TORQUE_FAULT: OVER_TORQUE_FAULT,
    LitterBoxStatus.PAUSED: PAUSED,
    LitterBoxStatus.PINCH_DETECT: PINCH_DETECT,
    LitterBoxStatus.POWER_DOWN: POWER_DOWN,
    LitterBoxStatus.POWER_UP: POWER_UP,
    LitterBoxStatus.READY: READY,
    LitterBoxStatus.STARTUP_CAT_SENSOR_FAULT: STARTUP_CAT_SENSOR_FAULT,
    LitterBoxStatus.STARTUP_DRAWER_FULL: STARTUP_DRAWER_FULL,
    LitterBoxStatus.STARTUP_PINCH_DETECT: STARTUP_PINCH_DETECT,
    LitterBoxStatus.UNKNOWN: UNKNOWN,
}

