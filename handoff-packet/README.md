# ETI360 Internal Apps (Weather + Sunlight) — Handoff Packet

This folder is a self-contained handoff for an internal system that:

- Stores canonical location + weather inputs in PostgreSQL
- Generates ETI360-branded chart **assets** (PNGs)
- Stores assets in AWS S3 and metadata in Postgres
- Stores internal planning documents in Postgres (`/documents/ui`)
- Runs entirely on Render (no Docker required)

## What runs where

- **Render Web Service (FastAPI API):** serves internal UIs + API, generates charts, uploads to S3, writes DB
- **Render Postgres:** source of truth for structured data
- **AWS S3:** durable storage for PNG chart assets under a prefix
- **Sanity:** marketing website content only (not part of this operational pipeline)

Note: This packet originally assumed an API + worker job queue. The current implementation runs generation synchronously inside the API request. A background worker can be added later if batch jobs start timing out.

## Files in this packet

- `architecture.md` — high-level design and data flow
- `schema.sql` — baseline Postgres schema (schemas: `weather`, `ops`)
- `env-vars.md` — required environment variables for the API service
- `api-contract.md` — current endpoints, auth, and roles
- `runbooks.md` — operational steps (deploy, debug, rotate secrets)
- `decisions.md` — key decisions, terminology, and best practices
- `how-dan-works.md` — collaboration tips for AI agents working with Dan
- `Working With Dan.md` — short entrypoint (links to `how-dan-works.md`)
- `Global Header.md` — optional codemod standard for file headers (run only when explicitly requested)

## Terminology (project language)

- Use **assets** (not artifacts) for generated PNG outputs.
- Use **weather_temp_precipitation** (not “normals”).
