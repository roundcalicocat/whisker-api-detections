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
    "detections": {
        "weight_drop_threshold": 0.5,  # lbs drop between early/recent halves to flag at_risk
        "weight_trajectory_days": 14,  # full window; split into two halves to detect gradual drift
        "weight_stddev_multiplier": 2, # standard dev multiplier, how many deviations to trigger
        "spike_window_hours": 2,       # rolling window to count visits for spike detection
        "spike_visit_threshold": 3,    # visits within window to flag as spike
        "usage_increase_days": 14,     # full window; split into two halves to detect gradual increase
        "usage_increase_threshold": 3  # visit count increase between weeks
    },
}