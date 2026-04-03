"""
Set cat names, weight (in lbs), and acceptable daily weight deviations (in lbs)

The LitterRobot weight sensor isn't accurate, so start at at weight_range of 0.5
"""

SETTINGS = {
    "cats": {
        "zahra": {
            "avg_weight": 4.0,      
            "weight_range": 1    
        },
        "ham": {
            "avg_weight": 13.5,
            "weight_range": 1
        }
    },
    "litter_robot": {
        "cycle_delay" : 8              # cycle delay in minutes, from configuration in whisker app
    },
    "detections": {
        "lookback_days": 6,           # shared lookback window across detections
        "weight_drop_threshold": 0.5,  # lbs drop between early/recent halves to flag at_risk
        "weight_stddev_multiplier": 2, # stddev multiplier for weight outlier exclusion
        "spike_window": "2 hours",     # rolling window to count visits for spike detection, can be measured in minutes or hours
        "spike_visit_threshold": 3,    # visits within window to flag as spike
        "usage_increase_threshold": 0.5, # min slope (visits/day) to flag upward trend
        "duration_stddev_multiplier": 2, # stddev multiplier for visit duration anomaly
        "daily_usage_threshold": 4     # typical daily usage count
    },
    "email": {
        "sender": "",
        "recipient": ""
    }
}