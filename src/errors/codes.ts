// src/errors/codes.ts

export const ERROR_CODES = {
  NO_PROPERTIES_FOUND:      { retryable: false, http: 404 },
  CONTACT_NOT_AVAILABLE:    { retryable: false, http: 200 },
  ENTITY_AMBIGUOUS:         { retryable: false, http: 200 },
  UPSTREAM_UNAVAILABLE:     { retryable: true,  http: 200 },
  RATE_LIMIT_EXCEEDED:      { retryable: true,  http: 429 },
  AUTH_REQUIRED:            { retryable: false, http: 402 },
  SCHEMA_VALIDATION_FAIL:   { retryable: false, http: 500 },
  JURISDICTION_UNSUPPORTED: { retryable: false, http: 422 },
  TIMEOUT:                  { retryable: true,  http: 200 },
} as const;

export type ErrorCode = keyof typeof ERROR_CODES;

export function structuredError(
  code: ErrorCode,
  message: string,
  partialData?: Record<string, unknown>
) {
  const { retryable } = ERROR_CODES[code];
  const payload: Record<string, unknown> = {
    error: { code, message, retryable, fallback_used: !!partialData },
    ...partialData,
  };
  return {
    content:          [{ type: 'text' as const, text: JSON.stringify(payload) }],
    structuredContent: payload,
    isError:           true,
  };
}
