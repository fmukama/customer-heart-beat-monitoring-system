from dataclasses import dataclass


@dataclass
class WindowState:

    count: int = 0

    heart_rate_sum: int = 0

    minimum: int | None = None

    maximum: int | None = None

    abnormal_count: int = 0

    def add(
        self,
        heart_rate: int,
        is_abnormal: bool,
    ) -> None:

        self.count += 1

        self.heart_rate_sum += heart_rate

        if self.minimum is None:
            self.minimum = heart_rate
        else:
            self.minimum = min(
                self.minimum,
                heart_rate,
            )

        if self.maximum is None:
            self.maximum = heart_rate
        else:
            self.maximum = max(
                self.maximum,
                heart_rate,
            )

        if is_abnormal:
            self.abnormal_count += 1

    @property
    def average(self) -> float:

        if self.count == 0:
            return 0.0

        return (
            self.heart_rate_sum
            / self.count
        )