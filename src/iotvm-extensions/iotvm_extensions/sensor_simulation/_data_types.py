from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, List, Literal


@dataclass
class Measurement:
    name: str
    value: Optional[float]
    unit: Optional[str]

    def to_dict(self) -> Dict:
        return asdict(obj=self)


@dataclass
class IntendedSimulatedSensorOperation:
    sensor_id: str
    macro: str


@dataclass
class EvaluatedSimulatedSensorOperation:
    operation: Literal["push"]
    fail: bool
    sensor_id: str
    measurements: List[Measurement]
    timestamp: int
    additional: Dict[str, Any]

    def to_print_string(self) -> str:
        return f"{'fail ' if self.fail is True else ''}{self.operation} to {self.sensor_id} : " + ", ".join(
            list(map(lambda i: f"{i.name}={i.value} {i.unit}", self.measurements))
        )


@dataclass
class RecurringWindow:
    number: int
    intended_ops: List[IntendedSimulatedSensorOperation]
    evaluated_ops: List[EvaluatedSimulatedSensorOperation]

    def to_print_string(self) -> str:
        string: str = f"RecurringWindow : {self.number}"
        for op in self.evaluated_ops:
            string2: str = ", ".join(list(map(lambda m: f"{m.name}={m.value} {m.unit}", op.measurements)))
            string = string + f"\n\t {op.sensor_id} : {string2})"
        return string
