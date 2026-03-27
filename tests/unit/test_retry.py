import pytest

from extg_shared.contracts.errors import RecoverableItemError
from extg_shared.utils.retry import AsyncRetryPolicy


@pytest.mark.asyncio
async def test_retry_policy_retries_with_backoff():
    attempts = 0
    sleeps = []

    async def operation():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RecoverableItemError("temporary")
        return "ok"

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    policy = AsyncRetryPolicy(
        max_attempts=3,
        base_delay_seconds=0.1,
        max_delay_seconds=1.0,
        jitter_seconds=0.0,
        sleep=fake_sleep,
    )

    result = await policy.run(operation)

    assert result == "ok"
    assert attempts == 3
    assert sleeps == [0.1, 0.2]
