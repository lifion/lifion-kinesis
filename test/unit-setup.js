import { vi } from 'vitest';

// jest auto-applied the manual mocks in __mocks__ to every test's module graph
// (they sit adjacent to the resolved root). Vitest does not automock node modules,
// so apply them once here for the whole unit suite. Each vi.mock with no factory
// resolves to the matching file in the repo-root __mocks__ dir.
vi.mock('async-retry');
vi.mock('aws-sdk');
vi.mock('got');
vi.mock('lifion-aws-event-stream');
vi.mock('short-uuid');
