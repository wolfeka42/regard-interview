# Architecture diagrams

## Data flow

```mermaid
flowchart LR
  bronze[(Bronze CSV<br/>encounters, labs, providers<br/>raw, PHI)] -->|explicit StructType,<br/>dedupe, sentinels, formatting,<br/>HMAC customer:patient_id| silver[(Silver<br/>clean_encounters, clean_labs,<br/>clean_providers)]
  silver -->|exclude cancelled + retracted,<br/>join, aggregate per provider-day| gold[(Gold<br/>provider_daily_activity)]
  salt[[salt<br/>env var or DEFAULT_SALT]] -.-> silver
  gold --> q["How busy was each provider<br/>on each day?"]
```

## Development flow

```mermaid
flowchart LR
  help[bmad-help] --> correct[Course correction<br/>after data review]
  correct --> build[bmad-build]
  build --> review[Review layers<br/>incl. PHI / HIPAA]
  review -->|findings| build
  review --> done[Tests pass,<br/>gold table produced]
```
