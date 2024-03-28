from typing import Optional, List

import numpy as np
import pandas as pd

from iotvm_extensions.constants import SEED
from ._base import (
    t_min_to_sec,
    DistributionType,
    ConstantDistribution,
    NormalDistribution,
    ExponentialDistribution,
    MeasurementBasicGenerator,
    MultiMeasurementCombiner,
    Loss,
    Frequency,
    Sensor,
    Measurement,
    Interactions,
    Variation,
    Iteration,
)

CONSTANT_FREQUENCY_5MIN_DISTRIBUTION: DistributionType = ConstantDistribution(
    seed_name=MeasurementBasicGenerator.X_SEED_NAME, loc=t_min_to_sec(5.0), size=None
)
CONSTANT_FREQUENCY_5MIN: Frequency = Frequency(distribution=CONSTANT_FREQUENCY_5MIN_DISTRIBUTION, seed=SEED)


class Presets:
    def __init__(self) -> None:
        self.sample: np.ndarray = ...
        self.start: pd.Timestamp = ...
        self.timezone: Optional[str] = None

    def prepare(self) -> None:
        self._sensor()
        self._loss()
        self._loss_seed()

    def _sensor(self) -> None:
        self.PAPER_SENSOR1: Sensor = Sensor(
            name="sensor-1",
            measurements=[
                Measurement(
                    name="temperature",
                    unit="celsius",
                    sample=self.sample,
                    frequency=CONSTANT_FREQUENCY_5MIN,
                    start=self.start,
                    timezone=self.timezone,
                    interactions=Interactions(
                        distributions=[
                            ConstantDistribution(
                                seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                loc=1.0,
                            ),
                            NormalDistribution(
                                seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                loc=0.5,
                                scale=0.2,
                                size=None,
                            ),
                        ],
                        seed=1,
                    ),
                ),
            ],
            frequency=None,
        )
        self.PAPER_SENSOR2: Sensor = Sensor(
            name="sensor-2",
            measurements=[
                Measurement(
                    name="temperature",
                    unit="celsius",
                    sample=self.sample,
                    frequency=CONSTANT_FREQUENCY_5MIN,
                    start=self.start,
                    timezone=self.timezone,
                    interactions=Interactions(
                        distributions=[
                            ConstantDistribution(
                                seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                loc=-1.0,
                                size=None,
                            ),
                            NormalDistribution(
                                seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                loc=0.5,
                                scale=0.1,
                                size=None,
                            ),
                        ],
                        seed=2,
                    ),
                ),
            ],
            frequency=None,
        )
        self.PAPER_SENSOR3: Sensor = Sensor(
            name="sensor-3",
            measurements=[
                Measurement(
                    name="temperature",
                    unit="celsius",
                    sample=self.sample,
                    frequency=CONSTANT_FREQUENCY_5MIN,
                    start=self.start,
                    timezone=self.timezone,
                    interactions=Interactions(
                        distributions=[
                            NormalDistribution(
                                seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                loc=0.0,
                                scale=2.0,
                                size=None,
                            ),
                        ],
                        seed=3,
                    ),
                ),
            ],
            frequency=None,
        )
        self.PAPER_SENSOR4: Sensor = Sensor(
            name="sensor-4",
            measurements=[
                Measurement(
                    name="temperature",
                    unit="celsius",
                    sample=self.sample,
                    frequency=CONSTANT_FREQUENCY_5MIN,
                    start=self.start,
                    timezone=self.timezone,
                    interactions=Interactions(
                        distributions=[
                            ConstantDistribution(
                                seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                loc=4.0,
                                size=None,
                            ),
                            NormalDistribution(
                                seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                loc=0.0,
                                scale=2.0,
                                size=None,
                            ),
                        ],
                        seed=4,
                    ),
                ),
            ],
            frequency=None,
        )
        self.PAPER_SENSOR5: Sensor = Sensor(
            name="sensor-5",
            measurements=[
                Measurement(
                    name="temperature",
                    unit="celsius",
                    sample=self.sample,
                    frequency=CONSTANT_FREQUENCY_5MIN,
                    start=self.start,
                    timezone=self.timezone,
                    interactions=Interactions(
                        distributions=[
                            ConstantDistribution(
                                seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                loc=-0.5,
                                size=None,
                            ),
                            NormalDistribution(
                                seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                loc=0.2,
                                scale=0.2,
                                size=None,
                            ),
                        ],
                        seed=5,
                    ),
                ),
            ],
            frequency=None,
        )
        self.PAPER_SENSOR6: Sensor = Sensor(
            name="sensor-6",
            measurements=[
                Measurement(
                    name="temperature",
                    unit="celsius",
                    sample=self.sample,
                    frequency=CONSTANT_FREQUENCY_5MIN,
                    start=self.start,
                    timezone=self.timezone,
                    interactions=Interactions(
                        distributions=[
                            ConstantDistribution(
                                seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                loc=0.5,
                                size=None,
                            ),
                            NormalDistribution(
                                seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                loc=0.2,
                                scale=0.2,
                                size=None,
                            ),
                        ],
                        seed=6,
                    ),
                ),
            ],
            frequency=None,
        )

        self.PAPER_SENSORS: List[Sensor] = [
            self.PAPER_SENSOR1,
            self.PAPER_SENSOR2,
            self.PAPER_SENSOR3,
            self.PAPER_SENSOR4,
            self.PAPER_SENSOR5,
            self.PAPER_SENSOR6,
        ]

    def _loss(self) -> None:
        self.PAPER_LOSS1: Loss = Loss(
            time_between_errors_distribution=ExponentialDistribution(
                seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(17.0), size=None
            ),
            error_duration_distribution=ExponentialDistribution(
                seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(33.0), size=None
            ),
        )
        self.PAPER_LOSS2: Loss = Loss(
            time_between_errors_distribution=ExponentialDistribution(
                seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(47.0), size=None
            ),
            error_duration_distribution=ExponentialDistribution(
                seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(47.0), size=None
            ),
        )
        self.PAPER_LOSS3: Loss = Loss(
            time_between_errors_distribution=ExponentialDistribution(
                seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(19.0), size=None
            ),
            error_duration_distribution=ExponentialDistribution(
                seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(29.0), size=None
            ),
        )
        self.PAPER_LOSS4: Loss = Loss(
            time_between_errors_distribution=ExponentialDistribution(
                seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(118.0), size=None
            ),
            error_duration_distribution=ExponentialDistribution(
                seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(15.0), size=None
            ),
        )
        self.PAPER_LOSS5: Loss = Loss(
            time_between_errors_distribution=ExponentialDistribution(
                seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(168.0), size=None
            ),
            error_duration_distribution=ExponentialDistribution(
                seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(38.0), size=None
            ),
        )
        self.PAPER_LOSS6: Loss = Loss(
            time_between_errors_distribution=ExponentialDistribution(
                seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(157.0), size=None
            ),
            error_duration_distribution=ExponentialDistribution(
                seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(10.0), size=None
            ),
        )

    def _loss_seed(self) -> None:
        self.PAPER_LOSS1_SEED: int = 862551904
        self.PAPER_LOSS2_SEED: int = 672927785
        self.PAPER_LOSS3_SEED: int = 173395504
        self.PAPER_LOSS4_SEED: int = 440574014
        self.PAPER_LOSS5_SEED: int = 888738903
        self.PAPER_LOSS6_SEED: int = 919622034
        
    def _get_loss_by_number(self, num: int) -> Loss:
        return getattr(self, f"PAPER_LOSS{num}")
        
    def _get_seed_by_number(self, num: int) -> int:
        return getattr(self, f"PAPER_LOSS{num}_SEED")
    
    def variation(self, name: str, sensor1: int, sensor2: int, sensor3: int, sensor4: int, sensor5: int, sensor6: int) -> Variation:
        return Variation(
            name=name,
            loss_by_sensor={
                "sensor-1": self._get_loss_by_number(num=sensor1),
                "sensor-2": self._get_loss_by_number(num=sensor2),
                "sensor-3": self._get_loss_by_number(num=sensor3),
                "sensor-4": self._get_loss_by_number(num=sensor4),
                "sensor-5": self._get_loss_by_number(num=sensor5),
                "sensor-6": self._get_loss_by_number(num=sensor6),
            },
            iterations=[
                Iteration(
                    name="iteration-1",
                    loss_seed_by_sensor={
                        "sensor-1": self._get_seed_by_number(num=sensor1),
                        "sensor-2": self._get_seed_by_number(num=sensor2),
                        "sensor-3": self._get_seed_by_number(num=sensor3),
                        "sensor-4": self._get_seed_by_number(num=sensor4),
                        "sensor-5": self._get_seed_by_number(num=sensor5),
                        "sensor-6": self._get_seed_by_number(num=sensor6),
                    },
                    loss_seed_fallback=SEED,
                ),
            ],
        )
