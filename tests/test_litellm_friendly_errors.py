"""Tests for friendly user-facing messages on terminal OpenRouter errors.

When OpenRouter rejects a call for a terminal reason — credits/weekly limit
exhausted (402), invalid key (401), or forbidden (403) — the user should see a
plain-language explanation instead of a raw exception string, and retrying must
not be attempted (those codes are not transient).

Transient codes (429, 5xx) must keep their raw error string so the retry layer
in LLMProvider.chat_with_retry still recognises and retries them.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import litellm
import pytest

from nanobot.providers.litellm_provider import LiteLLMProvider


def _provider() -> LiteLLMProvider:
    return LiteLLMProvider(
        api_key="sk-or-test-key",
        api_base="https://openrouter.ai/api/v1",
        default_model="anthropic/claude-sonnet-4-5",
        provider_name="openrouter",
    )


def _api_error(status_code: int, message: str) -> litellm.exceptions.APIError:
    return litellm.exceptions.APIError(
        status_code=status_code,
        message=message,
        llm_provider="openrouter",
        model="anthropic/claude-sonnet-4-5",
    )


@pytest.mark.asyncio
async def test_402_returns_friendly_quota_message() -> None:
    err = _api_error(402, "Insufficient credits")
    with patch("nanobot.providers.litellm_provider.acompletion", AsyncMock(side_effect=err)):
        response = await _provider().chat(messages=[{"role": "user", "content": "hi"}])

    assert response.finish_reason == "error"
    assert "Error calling LLM" not in response.content
    assert "額度" in response.content
    assert "allowance" in response.content.lower()


@pytest.mark.asyncio
async def test_401_returns_friendly_key_message() -> None:
    err = litellm.exceptions.AuthenticationError(
        message="No auth credentials found",
        llm_provider="openrouter",
        model="anthropic/claude-sonnet-4-5",
    )
    with patch("nanobot.providers.litellm_provider.acompletion", AsyncMock(side_effect=err)):
        response = await _provider().chat(messages=[{"role": "user", "content": "hi"}])

    assert response.finish_reason == "error"
    assert "Error calling LLM" not in response.content
    assert "金鑰" in response.content


@pytest.mark.asyncio
async def test_403_returns_friendly_forbidden_message() -> None:
    err = _api_error(403, "Forbidden")
    with patch("nanobot.providers.litellm_provider.acompletion", AsyncMock(side_effect=err)):
        response = await _provider().chat(messages=[{"role": "user", "content": "hi"}])

    assert response.finish_reason == "error"
    assert "Error calling LLM" not in response.content


@pytest.mark.asyncio
async def test_friendly_messages_are_not_transient() -> None:
    """Friendly terminal messages must not trip the transient-retry markers."""
    for status in (401, 402, 403):
        err = _api_error(status, "terminal")
        with patch("nanobot.providers.litellm_provider.acompletion", AsyncMock(side_effect=err)):
            response = await _provider().chat(messages=[{"role": "user", "content": "hi"}])
        assert not LiteLLMProvider._is_transient_error(response.content), (
            f"status {status} friendly message wrongly classified as transient"
        )


@pytest.mark.asyncio
async def test_429_keeps_raw_error_for_retry() -> None:
    """Rate-limit (429) stays a raw error string so chat_with_retry retries it."""
    err = litellm.exceptions.RateLimitError(
        message="rate limit exceeded",
        llm_provider="openrouter",
        model="anthropic/claude-sonnet-4-5",
    )
    with patch("nanobot.providers.litellm_provider.acompletion", AsyncMock(side_effect=err)):
        response = await _provider().chat(messages=[{"role": "user", "content": "hi"}])

    assert response.finish_reason == "error"
    assert "Error calling LLM" in response.content
    assert LiteLLMProvider._is_transient_error(response.content)
