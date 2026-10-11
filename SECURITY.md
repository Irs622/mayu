# Security policy

Please report vulnerabilities privately through
[GitHub security advisories](https://github.com/Siddid-Soni/mayu/security/advisories/new),
not in a public issue. Include the affected version, a description and, if you can, a reproduction.
You'll get an acknowledgement within a few days.

Only the latest release receives security fixes while mayu is pre-1.0.

Note that mayu deserializes task payloads from Redis. Anyone who can write to your Redis can
enqueue tasks, so treat Redis access as code-execution access to your workers, and don't expose it
to untrusted networks.
