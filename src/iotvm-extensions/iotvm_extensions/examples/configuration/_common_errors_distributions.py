from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class CommonErrorDistribution:
    ttp_between_errors_distribution: Dict[str, Any]
    error_ttp_distribution: Dict[str, Any]
    errors_distributions_seed: int


CED1: CommonErrorDistribution = CommonErrorDistribution(
    # Σε δεδομένα πλήθους 50, διάρκειας 5, δίνει error rate: TODO add.
    # Χρησιμοποιήθηκε στο SIMPAT-D-23-1602 στον sensor-1
    ttp_between_errors_distribution={
        "type": "exponential",
        "scale": 17.0,
    },
    error_ttp_distribution={
        "type": "exponential",
        "scale": 33.5,
    },
    errors_distributions_seed=862551904,
)
CED2: CommonErrorDistribution = CommonErrorDistribution(
    # Σε δεδομένα πλήθους 50, διάρκειας 5, δίνει error rate: TODO add.
    # Χρησιμοποιήθηκε στο SIMPAT-D-23-1602 στον sensor-2
    ttp_between_errors_distribution={
        "type": "exponential",
        "scale": 47.0,
    },
    error_ttp_distribution={
        "type": "exponential",
        "scale": 47.0,
    },
    errors_distributions_seed=672927785,
)
CED3: CommonErrorDistribution = CommonErrorDistribution(
    # Σε δεδομένα πλήθους 50, διάρκειας 5, δίνει error rate: TODO add.
    # Χρησιμοποιήθηκε στο SIMPAT-D-23-1602 στον sensor-3
    ttp_between_errors_distribution={
        "type": "exponential",
        "scale": 19.0,
    },
    error_ttp_distribution={
        "type": "exponential",
        "scale": 29.0,
    },
    errors_distributions_seed=173395504,
)
CED4: CommonErrorDistribution = CommonErrorDistribution(
    # Σε δεδομένα πλήθους 50, διάρκειας 5, δίνει error rate: TODO add.
    # Χρησιμοποιήθηκε στο SIMPAT-D-23-1602 στον sensor-4
    ttp_between_errors_distribution={
        "type": "exponential",
        "scale": 118.0,
    },
    error_ttp_distribution={
        "type": "exponential",
        "scale": 15.0,
    },
    errors_distributions_seed=440574014,
)
CED5: CommonErrorDistribution = CommonErrorDistribution(
    # Σε δεδομένα πλήθους 50, διάρκειας 5, δίνει error rate: TODO add.
    # Χρησιμοποιήθηκε στο SIMPAT-D-23-1602 στον sensor-5
    ttp_between_errors_distribution={
        "type": "exponential",
        "scale": 168.0,
    },
    error_ttp_distribution={
        "type": "exponential",
        "scale": 38.0,
    },
    errors_distributions_seed=888738903,
)
CED6: CommonErrorDistribution = CommonErrorDistribution(
    # Σε δεδομένα πλήθους 50, διάρκειας 5, δίνει error rate: TODO add.
    # Χρησιμοποιήθηκε στο SIMPAT-D-23-1602 στον sensor-6
    ttp_between_errors_distribution={
        "type": "exponential",
        "scale": 157.0,
    },
    error_ttp_distribution={
        "type": "exponential",
        "scale": 10.0,
    },
    errors_distributions_seed=919622034,
)
