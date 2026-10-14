# Pipeline output

- Run start (UTC): 2026-10-14T20:09:53Z
- Duration: 6.3 s
- Commit: `462991376904de44a6fdb5db49f053a345bb8bc0 (dirty tree)`
- Result: **PASS** (exit code 0)
- Data: synthetic only. Every layer is treated as PHI; bronze is counted, never shown.

## Rows per layer

| Layer | File | Rows |
|---|---|---|
| bronze | encounters.csv | 78 |
| bronze | labs.csv | 83 |
| bronze | providers.csv | 22 |
| silver | clean_encounters.csv | 70 |
| silver | clean_labs.csv | 77 |
| silver | clean_providers.csv | 17 |
| gold | provider_daily_activity.csv | 36 |

## Job counts (from the counts-only log)

| Layer | Source | Rows in | Rows out | Dropped |
|---|---|---|---|---|
| silver | encounters | 78 | 70 | 8 |
| silver | labs | 83 | 77 | 6 |
| silver | providers | 22 | 17 | 5 |
| gold | provider_daily_activity | 70 | 36 | 34 |

## Tables

### silver/clean_encounters.csv

| customer | encounter_id | patient_id | provider_id | start_time | end_time | status | duration_days | hashedpatientid | partition_timestamp |
|---|---|---|---|---|---|---|---|---|---|
| alpha | 1001 |  | 1 | 2024-01-05T08:00:00Z | 2024-01-05T14:00:00Z | completed | 0.25 | 042e3eefb0ae5fdc17d6056b1cf661eacbc1ed050f5ce3cdb6c7bdd9bc57d249 | 2026-10-14T20:09:54Z |
| alpha | 1002 |  | 1 | 2024-01-05T09:00:00Z | 2024-01-05T21:00:00Z | completed | 0.5 | 93ba130786f6d5fe470e78f63b239db190a6a04364db7a50db1e6bd5c09bdc33 | 2026-10-14T20:09:54Z |
| alpha | 1003 |  | 1 | 2024-01-06T10:00:00Z | 2024-01-07T10:00:00Z | completed | 1.0 | 7dbbe558852ccba8e984d3ffa74498c671651f58f8dacdc27bbe1a6bd0788dfa | 2026-10-14T20:09:54Z |
| alpha | 1003 |  | 2 | 2024-01-06T10:00:00Z | 2024-01-07T10:00:00Z | completed | 1.0 | 7dbbe558852ccba8e984d3ffa74498c671651f58f8dacdc27bbe1a6bd0788dfa | 2026-10-14T20:09:54Z |
| alpha | 1004 |  | 2 | 2024-01-07T08:00:00Z | 2024-01-07T20:00:00Z | completed | 0.5 | be3d5705a86a7142a36b65d9be1accf218c66ecf1430c583964b99ca353deee5 | 2026-10-14T20:09:54Z |
| alpha | 1005 |  | 1 | 2024-01-05T11:00:00Z | 2024-01-05T12:00:00Z | cancelled | 0.041666666666666664 | 5cba6945e55ce99606eaa33012b113285958eff0c360e93b6f5cf814f72a317c | 2026-10-14T20:09:54Z |
| alpha | 1006 |  | 3 | 2024-01-08T11:00:00Z | 2024-01-08T12:00:00Z | canceled | 0.041666666666666664 | 0199bb53eb7f51ce063edb72c48e0dae9fe13b6372983b59ae9a71e12d8515ff | 2026-10-14T20:09:54Z |
| alpha | 1007 |  | 2 | 2024-01-08T08:00:00Z |  | completed |  | 21159d4621c78ed4f0b74e19017e1826bfbab51c316d34ceec4b505185be74d8 | 2026-10-14T20:09:54Z |
| alpha | 1008 |  | 2 | 2024-01-08T10:00:00Z | 2024-01-08T09:00:00Z | completed |  |  | 2026-10-14T20:09:54Z |
| alpha | 1009 |  | 99 | 2024-01-05T13:00:00Z | 2024-01-05T19:00:00Z | completed | 0.25 | 042e3eefb0ae5fdc17d6056b1cf661eacbc1ed050f5ce3cdb6c7bdd9bc57d249 | 2026-10-14T20:09:54Z |
| alpha | 1014 |  | 3 | 2024-01-09T08:00:00Z |  |  |  | 1209670cdb00950692a79c4a460ab6442c5b53ab9896f50e540c87dd2c4cbb3e | 2026-10-14T20:09:54Z |
| alpha | 1015 |  | 3 | 2024-01-09T10:00:00Z | 2024-01-09T16:00:00Z |  | 0.25 |  | 2026-10-14T20:09:54Z |
| alpha | 7000 |  | 203 | 2024-02-02T08:00:00Z | 2024-02-02T11:00:00Z | cancelled | 0.125 | 3a5340d097d968b497a18bdb8c9881914f98798a1b40661fecb20fb115bf0b60 | 2026-10-14T20:09:54Z |
| alpha | 7001 |  | 200 | 2024-02-01T07:00:00Z | 2024-02-01T13:00:00Z | cancelled | 0.25 | f90494e125a7e5d8b9069c2b82592ca2ae5509c1053ffff648425b3043bcae79 | 2026-10-14T20:09:54Z |
| alpha | 7001 |  | 202 | 2024-02-01T07:00:00Z | 2024-02-01T13:00:00Z | cancelled | 0.25 | f90494e125a7e5d8b9069c2b82592ca2ae5509c1053ffff648425b3043bcae79 | 2026-10-14T20:09:54Z |
| alpha | 7002 |  | 201 | 2024-02-02T10:00:00Z | 2024-02-02T22:00:00Z | completed | 0.5 | 5b982bd36f5d1e17116ae0ebe728d1ebf3ccf08a1eccc25f02d01e0eb2d47fc3 | 2026-10-14T20:09:54Z |
| alpha | 7002 |  | 203 | 2024-02-02T10:00:00Z | 2024-02-02T22:00:00Z | completed | 0.5 | 5b982bd36f5d1e17116ae0ebe728d1ebf3ccf08a1eccc25f02d01e0eb2d47fc3 | 2026-10-14T20:09:54Z |
| alpha | 7003 |  | 203 | 2024-02-01T09:00:00Z | 2024-02-01T21:00:00Z | completed | 0.5 | d2b351096bd1c3668fa04ec9d44038e7a1a158f12eeda7ec2fe3d04d8ced6235 | 2026-10-14T20:09:54Z |
| alpha | 7004 |  | 201 | 2024-02-04T07:00:00Z | 2024-02-04T19:00:00Z | cancelled | 0.5 | 3a5340d097d968b497a18bdb8c9881914f98798a1b40661fecb20fb115bf0b60 | 2026-10-14T20:09:54Z |
| alpha | 7005 |  | 200 | 2024-02-01T12:00:00Z | 2024-02-01T18:00:00Z | completed | 0.25 | 48b34428dc859016865ec3e330803a7b556a4b4d802fc5c42016e6c81bf3c343 | 2026-10-14T20:09:54Z |
| alpha | 7006 |  | 203 | 2024-02-05T12:00:00Z | 2024-02-05T18:00:00Z | completed | 0.25 | 6df1eb1fd3c578887a1436a9bb9129dad6acec4804d6df89eb561ad8f3da1ead | 2026-10-14T20:09:54Z |
| alpha | 7007 |  | 200 | 2024-02-04T09:00:00Z | 2024-02-04T12:00:00Z | cancelled | 0.125 | f90494e125a7e5d8b9069c2b82592ca2ae5509c1053ffff648425b3043bcae79 | 2026-10-14T20:09:54Z |
| alpha | 7007 |  | 202 | 2024-02-04T09:00:00Z | 2024-02-04T12:00:00Z | cancelled | 0.125 | f90494e125a7e5d8b9069c2b82592ca2ae5509c1053ffff648425b3043bcae79 | 2026-10-14T20:09:54Z |
| alpha | 7008 |  | 201 | 2024-02-03T12:00:00Z | 2024-02-04T12:00:00Z | in-progress | 1.0 | d2b351096bd1c3668fa04ec9d44038e7a1a158f12eeda7ec2fe3d04d8ced6235 | 2026-10-14T20:09:54Z |
| alpha | 7009 |  | 202 | 2024-02-01T07:00:00Z | 2024-02-01T13:00:00Z | completed | 0.25 | 3a5340d097d968b497a18bdb8c9881914f98798a1b40661fecb20fb115bf0b60 | 2026-10-14T20:09:54Z |
| alpha | 7009 |  | 203 | 2024-02-01T07:00:00Z | 2024-02-01T13:00:00Z | completed | 0.25 | 3a5340d097d968b497a18bdb8c9881914f98798a1b40661fecb20fb115bf0b60 | 2026-10-14T20:09:54Z |
| alpha | 7010 |  | 200 | 2024-02-04T10:00:00Z | 2024-02-05T10:00:00Z | completed | 1.0 | 6df1eb1fd3c578887a1436a9bb9129dad6acec4804d6df89eb561ad8f3da1ead | 2026-10-14T20:09:54Z |
| alpha | 7010 |  | 202 | 2024-02-04T10:00:00Z | 2024-02-05T10:00:00Z | completed | 1.0 | 6df1eb1fd3c578887a1436a9bb9129dad6acec4804d6df89eb561ad8f3da1ead | 2026-10-14T20:09:54Z |
| alpha | 7011 |  | 201 | 2024-02-05T09:00:00Z | 2024-02-05T21:00:00Z | completed | 0.5 | e881a13991958200dcbf43959c475236b0d0a02739dd98a4708fe4c6d9dd53f2 | 2026-10-14T20:09:54Z |
| alpha | 7012 |  | 200 | 2024-02-05T08:00:00Z | 2024-02-05T14:00:00Z | completed | 0.25 | 44d2131d835acd832ba6f703b6032b0b43c35cb6590e505997dce681f840a2e7 | 2026-10-14T20:09:54Z |
| alpha | 7012 |  | 202 | 2024-02-05T08:00:00Z | 2024-02-05T14:00:00Z | completed | 0.25 | 44d2131d835acd832ba6f703b6032b0b43c35cb6590e505997dce681f840a2e7 | 2026-10-14T20:09:54Z |
| alpha | 7013 |  | 201 | 2024-02-01T08:00:00Z | 2024-02-01T11:00:00Z | in-progress | 0.125 | e881a13991958200dcbf43959c475236b0d0a02739dd98a4708fe4c6d9dd53f2 | 2026-10-14T20:09:54Z |
| alpha | 7014 |  | 201 | 2024-02-02T09:00:00Z | 2024-02-03T09:00:00Z | cancelled | 1.0 | f90494e125a7e5d8b9069c2b82592ca2ae5509c1053ffff648425b3043bcae79 | 2026-10-14T20:09:54Z |
| alpha | 7014 |  | 202 | 2024-02-02T09:00:00Z | 2024-02-03T09:00:00Z | cancelled | 1.0 | f90494e125a7e5d8b9069c2b82592ca2ae5509c1053ffff648425b3043bcae79 | 2026-10-14T20:09:54Z |
| alpha | 7015 |  | 201 | 2024-02-02T08:00:00Z | 2024-02-02T11:00:00Z | completed | 0.125 | 44d2131d835acd832ba6f703b6032b0b43c35cb6590e505997dce681f840a2e7 | 2026-10-14T20:09:54Z |
| alpha | 7015 |  | 202 | 2024-02-02T08:00:00Z | 2024-02-02T11:00:00Z | completed | 0.125 | 44d2131d835acd832ba6f703b6032b0b43c35cb6590e505997dce681f840a2e7 | 2026-10-14T20:09:54Z |
| alpha | 7016 |  | 201 | 2024-02-02T07:00:00Z | 2024-02-02T10:00:00Z | completed | 0.125 | 44d2131d835acd832ba6f703b6032b0b43c35cb6590e505997dce681f840a2e7 | 2026-10-14T20:09:54Z |
| alpha | 7016 |  | 203 | 2024-02-02T07:00:00Z | 2024-02-02T10:00:00Z | completed | 0.125 | 44d2131d835acd832ba6f703b6032b0b43c35cb6590e505997dce681f840a2e7 | 2026-10-14T20:09:54Z |
| alpha | 7017 |  | 203 | 2024-02-04T08:00:00Z | 2024-02-04T11:00:00Z | in-progress | 0.125 | e881a13991958200dcbf43959c475236b0d0a02739dd98a4708fe4c6d9dd53f2 | 2026-10-14T20:09:54Z |
| alpha | 7018 |  | 202 | 2024-02-02T11:00:00Z | 2024-02-03T11:00:00Z | in-progress | 1.0 | 066b341f0daebdda7659398b1f285040fbc67b5e1d1ea00b2c2502991f970601 | 2026-10-14T20:09:54Z |
| alpha | 7019 |  | 201 | 2024-02-01T08:00:00Z | 2024-02-01T14:00:00Z | in-progress | 0.25 | 0854e66c83988d87c19cda42e1118230edb2d494f16aea909887bc8acb87400d | 2026-10-14T20:09:54Z |
| beta | 00e85ece-0b49-452d-86d4-83f3d450281c |  | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | 2024-02-03T08:00:00Z | 2024-02-03T20:00:00Z | in-progress | 0.5 | df4c619f1bbd1eca9ef8d55016d0401fcd259337f8a7392461e185d52f29876d | 2026-10-14T20:09:54Z |
| beta | 00e85ece-0b49-452d-86d4-83f3d450281c |  | 944528c0-0ef8-42d6-b7fd-564637bb3eec | 2024-02-03T08:00:00Z | 2024-02-03T20:00:00Z | in-progress | 0.5 | df4c619f1bbd1eca9ef8d55016d0401fcd259337f8a7392461e185d52f29876d | 2026-10-14T20:09:54Z |
| beta | 22bd3388-6db9-4102-a48b-3dbe157d94a1 |  | b7e99aca-a970-45e1-8e46-d534c88a618e | 2024-02-04T08:00:00Z | 2024-02-04T11:00:00Z | in-progress | 0.125 | 32b8f0300c80e5a654b36ce9fc569acf6047f867429615f56fcc7877887b22e8 | 2026-10-14T20:09:54Z |
| beta | 43b409ef-2260-470f-a0cc-edc5f05db76e |  | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | 2024-02-01T07:00:00Z | 2024-02-01T13:00:00Z | completed | 0.25 | 32b8f0300c80e5a654b36ce9fc569acf6047f867429615f56fcc7877887b22e8 | 2026-10-14T20:09:54Z |
| beta | 43b409ef-2260-470f-a0cc-edc5f05db76e |  | b7e99aca-a970-45e1-8e46-d534c88a618e | 2024-02-01T07:00:00Z | 2024-02-01T13:00:00Z | completed | 0.25 | 32b8f0300c80e5a654b36ce9fc569acf6047f867429615f56fcc7877887b22e8 | 2026-10-14T20:09:54Z |
| beta | 4ccc9bc2-a53f-4a28-abf3-e3fc21813d25 |  | 944528c0-0ef8-42d6-b7fd-564637bb3eec | 2024-02-04T09:00:00Z | 2024-02-04T12:00:00Z | in-progress | 0.125 | 0fdb083c03481f62ff4166fca656a86ccd2d75b9507185dae9b7b64fe3d19185 | 2026-10-14T20:09:54Z |
| beta | 4ccc9bc2-a53f-4a28-abf3-e3fc21813d25 |  | fed4057d-bb02-4576-b512-c4c3b253d218 | 2024-02-04T09:00:00Z | 2024-02-04T12:00:00Z | in-progress | 0.125 | 0fdb083c03481f62ff4166fca656a86ccd2d75b9507185dae9b7b64fe3d19185 | 2026-10-14T20:09:54Z |
| beta | 587ef344-6f3f-420c-98b8-e4cc1bc044fc |  | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | 2024-02-03T10:00:00Z | 2024-02-03T13:00:00Z | cancelled | 0.125 | 96f085b75324498c0ea98ea53e26ff5aa25b3d4903a3c068e6de6e83daa66237 | 2026-10-14T20:09:54Z |
| beta | 6b8e869f-d538-4b0e-b4f3-193c0ff0a55c |  | fed4057d-bb02-4576-b512-c4c3b253d218 | 2024-02-04T11:00:00Z | 2024-02-04T14:00:00Z | in-progress | 0.125 | 9ced90506760998d5e406ea8cd95666b3691cfb89bc093c2a96750919f2e845e | 2026-10-14T20:09:54Z |
| beta | 6e0d2648-35ce-4841-8973-2d6c4dcabfb7 |  | b7e99aca-a970-45e1-8e46-d534c88a618e | 2024-02-05T11:00:00Z | 2024-02-05T23:00:00Z | in-progress | 0.5 | a1028285844cb2a9ded6e664c7dc73bc9d075e52485d5e459c38a0d8fe4371d7 | 2026-10-14T20:09:54Z |
| beta | 6f1c2d3e-0000-4000-8000-00000000b002 |  | c0ffee00-0000-4000-8000-0000000000d1 | 2024-01-06T09:00:00Z | 2024-01-06T21:00:00Z | completed | 0.5 | d64ff947cc25c1c0a302d64ea3c94745f1ae9a4755af42418888f90e63119cf9 | 2026-10-14T20:09:54Z |
| beta | 702cdd20-2862-48b8-88f4-ef125e9953d2 |  | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | 2024-02-05T12:00:00Z | 2024-02-06T00:00:00Z | cancelled | 0.5 | 9ced90506760998d5e406ea8cd95666b3691cfb89bc093c2a96750919f2e845e | 2026-10-14T20:09:54Z |
| beta | 7c5308bf-6f92-425e-85df-16b6382c043f |  | 944528c0-0ef8-42d6-b7fd-564637bb3eec | 2024-02-01T10:00:00Z | 2024-02-01T22:00:00Z | in-progress | 0.5 | 728b2623e7a651cdd7346423df5804ed0db7189eb8047e14186589c19c7482ac | 2026-10-14T20:09:54Z |
| beta | 7c5308bf-6f92-425e-85df-16b6382c043f |  | b7e99aca-a970-45e1-8e46-d534c88a618e | 2024-02-01T10:00:00Z | 2024-02-01T22:00:00Z | in-progress | 0.5 | 728b2623e7a651cdd7346423df5804ed0db7189eb8047e14186589c19c7482ac | 2026-10-14T20:09:54Z |
| beta | 85197ff4-006e-46e3-afa1-7735b572f3d0 |  | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | 2024-02-05T12:00:00Z | 2024-02-05T18:00:00Z | in-progress | 0.25 | 0ba85862029c89241e5d6776205f58e67956220104895468c8712e47a71cebbb | 2026-10-14T20:09:54Z |
| beta | 8a476a87-e49d-481d-91d8-7c6455fa1ab8 |  | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | 2024-02-01T08:00:00Z | 2024-02-01T14:00:00Z | in-progress | 0.25 | 81a87ace5366567068cf7116a6196643a972049323b2c34d496b4baf8cdeef13 | 2026-10-14T20:09:54Z |
| beta | 8a476a87-e49d-481d-91d8-7c6455fa1ab8 |  | b7e99aca-a970-45e1-8e46-d534c88a618e | 2024-02-01T08:00:00Z | 2024-02-01T14:00:00Z | in-progress | 0.25 | 81a87ace5366567068cf7116a6196643a972049323b2c34d496b4baf8cdeef13 | 2026-10-14T20:09:54Z |
| beta | aa38d0a1-6ba2-4efe-b11c-6eb62095eef6 |  | 944528c0-0ef8-42d6-b7fd-564637bb3eec | 2024-02-04T12:00:00Z | 2024-02-04T18:00:00Z | cancelled | 0.25 | 9ced90506760998d5e406ea8cd95666b3691cfb89bc093c2a96750919f2e845e | 2026-10-14T20:09:54Z |
| beta | aabc25fa-3fe1-4e47-ae9b-ec3635c7936c |  | b7e99aca-a970-45e1-8e46-d534c88a618e | 2024-02-01T09:00:00Z | 2024-02-02T09:00:00Z | completed | 1.0 | 9ced90506760998d5e406ea8cd95666b3691cfb89bc093c2a96750919f2e845e | 2026-10-14T20:09:54Z |
| beta | aae65fc1-76f2-4bfe-8d29-a36f222282e1 |  | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | 2024-02-05T11:00:00Z | 2024-02-05T23:00:00Z | cancelled | 0.5 | a1028285844cb2a9ded6e664c7dc73bc9d075e52485d5e459c38a0d8fe4371d7 | 2026-10-14T20:09:54Z |
| beta | aae65fc1-76f2-4bfe-8d29-a36f222282e1 |  | 944528c0-0ef8-42d6-b7fd-564637bb3eec | 2024-02-05T11:00:00Z | 2024-02-05T23:00:00Z | cancelled | 0.5 | a1028285844cb2a9ded6e664c7dc73bc9d075e52485d5e459c38a0d8fe4371d7 | 2026-10-14T20:09:54Z |
| beta | adf4e62d-6651-429e-8268-690ba43825b5 |  | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | 2024-02-05T09:00:00Z | 2024-02-05T12:00:00Z | completed | 0.125 | dcd1b239ae0aaddefc3de6d7f03ab345edbb5fc27eb1473d8fde6f4f4670b544 | 2026-10-14T20:09:54Z |
| beta | b-0001 |  | 1 | 2024-01-05T08:00:00Z | 2024-01-05T14:00:00Z | completed | 0.25 | ca87010db72bdbeeca803e515ff9c71fa9a71542833667215a071b926d47dfd0 | 2026-10-14T20:09:54Z |
| beta | b7e6427c-bf78-4e3f-b6b7-51f79b749245 |  | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | 2024-02-01T08:00:00Z | 2024-02-01T20:00:00Z | cancelled | 0.5 | dcd1b239ae0aaddefc3de6d7f03ab345edbb5fc27eb1473d8fde6f4f4670b544 | 2026-10-14T20:09:54Z |
| beta | bc2cbb0d-dd33-4cc7-ab7f-089acd5f4822 |  | fed4057d-bb02-4576-b512-c4c3b253d218 | 2024-02-02T09:00:00Z | 2024-02-02T15:00:00Z | completed | 0.25 | fd6afc776ea6b6d3c03b8b86c6077daf26543b58d064fac9247399d241f8201d | 2026-10-14T20:09:54Z |
| beta | d6d7b3b8-3309-4d35-bf4d-f561f319c125 |  | fed4057d-bb02-4576-b512-c4c3b253d218 | 2024-02-01T11:00:00Z | 2024-02-01T17:00:00Z | completed | 0.25 | 81a87ace5366567068cf7116a6196643a972049323b2c34d496b4baf8cdeef13 | 2026-10-14T20:09:54Z |
| beta | d98868dd-9c7c-4377-b9a2-8903fbe33b24 |  | 944528c0-0ef8-42d6-b7fd-564637bb3eec | 2024-02-01T10:00:00Z | 2024-02-02T10:00:00Z | cancelled | 1.0 | 728b2623e7a651cdd7346423df5804ed0db7189eb8047e14186589c19c7482ac | 2026-10-14T20:09:54Z |
| beta | e21342b0-f1ee-4ba3-9343-2e611ca3c448 |  | 944528c0-0ef8-42d6-b7fd-564637bb3eec | 2024-02-02T11:00:00Z | 2024-02-02T14:00:00Z | cancelled | 0.125 | 0ba85862029c89241e5d6776205f58e67956220104895468c8712e47a71cebbb | 2026-10-14T20:09:54Z |
| beta | e21342b0-f1ee-4ba3-9343-2e611ca3c448 |  | b7e99aca-a970-45e1-8e46-d534c88a618e | 2024-02-02T11:00:00Z | 2024-02-02T14:00:00Z | cancelled | 0.125 | 0ba85862029c89241e5d6776205f58e67956220104895468c8712e47a71cebbb | 2026-10-14T20:09:54Z |

### silver/clean_labs.csv

| customer | lab_id | encounter_id | test_name | result_value | result_unit | observed_time | status | partition_timestamp |
|---|---|---|---|---|---|---|---|---|
| alpha | 5001 | 1001 | glucose | 95.0 | mg/dL | 2024-01-05T09:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 5002 | 1001 | sodium | 140.0 | mmol/L | 2024-01-05T09:05:00Z |  | 2026-10-14T20:09:54Z |
| alpha | 5003 | 1002 | potassium | 4.1 | mmol/L | 2024-01-05T10:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 5004 | 1002 | glucose | 300.0 | mg/dL | 2024-01-05T10:00:00Z | retracted | 2026-10-14T20:09:54Z |
| alpha | 5005 | 1003 | hemoglobin | 13.5 | g/dL | 2024-01-07T08:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 5006 | 1005 | glucose | 88.0 | mg/dL | 2024-01-05T11:30:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 5008 | 1004 | glucose |  | mg/dL | 2024-01-07T09:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 5009 | 1009 | sodium | 138.0 | mmol/L | 2024-01-05T14:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 5010 | 1014 | hemoglobin a1c | 4.1 |  | 2024-01-09T09:00:00Z |  | 2026-10-14T20:09:54Z |
| alpha | 5011 | 1015 | glucose |  | mg/dL | 2024-01-09T11:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 5012 | 1015 | hemoglobin a1c | 6.0 | % | 2024-01-09T11:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 5013 | 1015 | na | 139.0 | mmol/L | 2024-01-09T11:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90010 | 7001 | potassium | 280.0 | mmol/L | 2024-02-01T08:00:00Z |  | 2026-10-14T20:09:54Z |
| alpha | 90020 | 7002 | glucose | 111.0 | mg/dL | 2024-02-02T11:00:00Z | preliminary | 2026-10-14T20:09:54Z |
| alpha | 90021 | 7002 | glucose | 48.0 | mg/dL | 2024-02-02T11:00:00Z |  | 2026-10-14T20:09:54Z |
| alpha | 90050 | 7005 | sodium | 233.0 | mmol/L | 2024-02-01T13:00:00Z | preliminary | 2026-10-14T20:09:54Z |
| alpha | 90051 | 7005 | glucose | 190.0 | mg/dL | 2024-02-01T13:00:00Z | preliminary | 2026-10-14T20:09:54Z |
| alpha | 90052 | 7005 | glucose | 137.0 | mg/dL | 2024-02-01T13:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90060 | 7006 | sodium | 286.0 | mmol/L | 2024-02-05T13:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90061 | 7006 | potassium | 167.0 | mmol/L | 2024-02-05T13:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90062 | 7006 | glucose | 17.0 | mg/dL | 2024-02-05T13:00:00Z | preliminary | 2026-10-14T20:09:54Z |
| alpha | 90070 | 7007 | sodium | 235.0 | mmol/L | 2024-02-04T10:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90071 | 7007 | sodium | 72.0 | mmol/L | 2024-02-04T10:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90072 | 7007 | potassium | 288.0 | mmol/L | 2024-02-04T10:00:00Z | retracted | 2026-10-14T20:09:54Z |
| alpha | 90080 | 7008 | potassium | 253.0 | mmol/L | 2024-02-03T13:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90110 | 7011 | glucose | 135.0 | mg/dL | 2024-02-05T10:00:00Z | retracted | 2026-10-14T20:09:54Z |
| alpha | 90111 | 7011 | glucose | 260.0 | mg/dL | 2024-02-05T10:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90112 | 7011 | potassium | 153.0 | mmol/L | 2024-02-05T10:00:00Z | retracted | 2026-10-14T20:09:54Z |
| alpha | 90120 | 7012 | sodium | 10.0 | mmol/L | 2024-02-05T09:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90121 | 7012 | sodium | 158.0 | mmol/L | 2024-02-05T09:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90130 | 7013 | potassium | 244.0 | mmol/L | 2024-02-01T09:00:00Z | retracted | 2026-10-14T20:09:54Z |
| alpha | 90140 | 7014 | sodium | 192.0 | mmol/L | 2024-02-02T10:00:00Z |  | 2026-10-14T20:09:54Z |
| alpha | 90141 | 7014 | potassium | 232.0 | mmol/L | 2024-02-02T10:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90150 | 7015 | glucose | 37.0 | mg/dL | 2024-02-02T09:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90160 | 7016 | glucose | 277.0 | mg/dL | 2024-02-02T08:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90161 | 7016 | potassium | 293.0 | mmol/L | 2024-02-02T08:00:00Z | retracted | 2026-10-14T20:09:54Z |
| alpha | 90162 | 7016 | sodium | 125.0 | mmol/L | 2024-02-02T08:00:00Z |  | 2026-10-14T20:09:54Z |
| alpha | 90170 | 7017 | sodium | 28.0 | mmol/L | 2024-02-04T09:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90171 | 7017 | glucose | 207.0 | mg/dL | 2024-02-04T09:00:00Z | preliminary | 2026-10-14T20:09:54Z |
| alpha | 90172 | 7017 | glucose | 128.0 | mg/dL | 2024-02-04T09:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90180 | 7018 | glucose | 39.0 | mg/dL | 2024-02-02T12:00:00Z |  | 2026-10-14T20:09:54Z |
| alpha | 90181 | 7018 | potassium | 51.0 | mmol/L | 2024-02-02T12:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90182 | 7018 | potassium | 277.0 | mmol/L | 2024-02-02T12:00:00Z | final | 2026-10-14T20:09:54Z |
| alpha | 90190 | 7019 | glucose | 85.0 | mg/dL | 2024-02-01T09:00:00Z |  | 2026-10-14T20:09:54Z |
| alpha | 90191 | 7019 | glucose | 200.0 | mg/dL | 2024-02-01T09:00:00Z | preliminary | 2026-10-14T20:09:54Z |
| alpha | 90192 | 7019 | sodium | 147.0 | mmol/L | 2024-02-01T09:00:00Z |  | 2026-10-14T20:09:54Z |
| beta | 04f64d86-7866-4765-94f7-ce8dd5bcb8d0 | 22bd3388-6db9-4102-a48b-3dbe157d94a1 | sodium | 277.0 | mmol/L | 2024-02-04T09:00:00Z | final | 2026-10-14T20:09:54Z |
| beta | 0640be0f-25b8-4d4b-b2fa-2de8ce7ae7f6 | 6e0d2648-35ce-4841-8973-2d6c4dcabfb7 | glucose | 24.0 | mg/dL | 2024-02-05T12:00:00Z | final | 2026-10-14T20:09:54Z |
| beta | 0d01280f-d89a-40c0-a87d-1c78e7c421c7 | 43b409ef-2260-470f-a0cc-edc5f05db76e | sodium | 48.0 | mmol/L | 2024-02-01T08:00:00Z |  | 2026-10-14T20:09:54Z |
| beta | 13748146-32c5-4d89-b70b-3420f1043785 | b7e6427c-bf78-4e3f-b6b7-51f79b749245 | sodium | 125.0 | mmol/L | 2024-02-01T09:00:00Z | final | 2026-10-14T20:09:54Z |
| beta | 3170f437-a8f7-4f5a-860e-df5b39118497 | bc2cbb0d-dd33-4cc7-ab7f-089acd5f4822 | glucose | 205.0 | mg/dL | 2024-02-02T10:00:00Z | preliminary | 2026-10-14T20:09:54Z |
| beta | 439472e6-da58-4e8a-a25d-6b29afffcfd2 | 43b409ef-2260-470f-a0cc-edc5f05db76e | glucose | 259.0 | mg/dL | 2024-02-01T08:00:00Z |  | 2026-10-14T20:09:54Z |
| beta | 48a639d0-15b5-4908-a8aa-71582b70e525 | 6e0d2648-35ce-4841-8973-2d6c4dcabfb7 | potassium | 264.0 | mmol/L | 2024-02-05T12:00:00Z | retracted | 2026-10-14T20:09:54Z |
| beta | 49257af1-b6aa-405b-93d5-f2f7709b7d97 | aae65fc1-76f2-4bfe-8d29-a36f222282e1 | sodium | 121.0 | mmol/L | 2024-02-05T12:00:00Z | preliminary | 2026-10-14T20:09:54Z |
| beta | 5958a499-eeea-463e-a1e8-ac6843e42caf | 4ccc9bc2-a53f-4a28-abf3-e3fc21813d25 | potassium | 36.0 | mmol/L | 2024-02-04T10:00:00Z | final | 2026-10-14T20:09:54Z |
| beta | 5e6fea07-c453-4f1d-8199-2fdfb31022f0 | d6d7b3b8-3309-4d35-bf4d-f561f319c125 | sodium | 86.0 | mmol/L | 2024-02-01T12:00:00Z | retracted | 2026-10-14T20:09:54Z |
| beta | 610e6a64-e130-4617-82df-f33556666f9f | 22bd3388-6db9-4102-a48b-3dbe157d94a1 | sodium | 143.0 | mmol/L | 2024-02-04T09:00:00Z |  | 2026-10-14T20:09:54Z |
| beta | 64de82e6-e82c-4d7b-86e7-45f988bc539c | 7c5308bf-6f92-425e-85df-16b6382c043f | potassium | 289.0 | mmol/L | 2024-02-01T11:00:00Z | final | 2026-10-14T20:09:54Z |
| beta | 6703b636-5380-4904-a88c-7015aab97e49 | 85197ff4-006e-46e3-afa1-7735b572f3d0 | sodium | 152.0 | mmol/L | 2024-02-05T13:00:00Z | retracted | 2026-10-14T20:09:54Z |
| beta | 7337c599-7984-4388-9c8a-ee30be6033f7 | aae65fc1-76f2-4bfe-8d29-a36f222282e1 | glucose | 133.0 | mg/dL | 2024-02-05T12:00:00Z | final | 2026-10-14T20:09:54Z |
| beta | 75d66ed4-eb1f-49f2-910b-d1d033173470 | bc2cbb0d-dd33-4cc7-ab7f-089acd5f4822 | glucose | 180.0 | mg/dL | 2024-02-02T10:00:00Z | preliminary | 2026-10-14T20:09:54Z |
| beta | 8573e793-c715-42b9-840c-5d9146fde062 | aae65fc1-76f2-4bfe-8d29-a36f222282e1 | potassium | 249.0 | mmol/L | 2024-02-05T12:00:00Z | final | 2026-10-14T20:09:54Z |
| beta | 8ae8905b-54b4-4482-a858-6eba6a34c854 | 8a476a87-e49d-481d-91d8-7c6455fa1ab8 | glucose | 239.0 | mg/dL | 2024-02-01T09:00:00Z |  | 2026-10-14T20:09:54Z |
| beta | 9a6ec2f5-ccc4-4903-8bcf-53a1bc10fa52 | 6b8e869f-d538-4b0e-b4f3-193c0ff0a55c | potassium | 113.0 | mmol/L | 2024-02-04T12:00:00Z |  | 2026-10-14T20:09:54Z |
| beta | a9597663-6daa-4e68-8861-fe1858e25888 | b7e6427c-bf78-4e3f-b6b7-51f79b749245 | glucose | 190.0 | mg/dL | 2024-02-01T09:00:00Z | final | 2026-10-14T20:09:54Z |
| beta | af2b99b4-d9ac-4158-8d34-85c5c5c14eb4 | b7e6427c-bf78-4e3f-b6b7-51f79b749245 | potassium | 62.0 | mmol/L | 2024-02-01T09:00:00Z | retracted | 2026-10-14T20:09:54Z |
| beta | b00805cc-a7f3-4ae9-a5c7-3c443e75c3b4 | d98868dd-9c7c-4377-b9a2-8903fbe33b24 | sodium | 3.0 | mmol/L | 2024-02-01T11:00:00Z | final | 2026-10-14T20:09:54Z |
| beta | c04a96c4-f3b6-4fe1-9184-332417e8392a | 6e0d2648-35ce-4841-8973-2d6c4dcabfb7 | sodium | 121.0 | mmol/L | 2024-02-05T12:00:00Z | preliminary | 2026-10-14T20:09:54Z |
| beta | c5f8bc16-f786-4b50-91c5-8ef0dd463c09 | bc2cbb0d-dd33-4cc7-ab7f-089acd5f4822 | sodium | 143.0 | mmol/L | 2024-02-02T10:00:00Z | preliminary | 2026-10-14T20:09:54Z |
| beta | c7fee39f-1190-4938-a66f-d7f739669fa7 | 22bd3388-6db9-4102-a48b-3dbe157d94a1 | sodium | 21.0 | mmol/L | 2024-02-04T09:00:00Z | final | 2026-10-14T20:09:54Z |
| beta | d92c9227-eadf-4085-bfcb-75468eb22579 | d98868dd-9c7c-4377-b9a2-8903fbe33b24 | glucose | 63.0 | mg/dL | 2024-02-01T11:00:00Z |  | 2026-10-14T20:09:54Z |
| beta | e4429ebb-da7b-4095-a3d6-2a39c0e3befd | 6b8e869f-d538-4b0e-b4f3-193c0ff0a55c | sodium | 215.0 | mmol/L | 2024-02-04T12:00:00Z | retracted | 2026-10-14T20:09:54Z |
| beta | e6697833-b841-40a0-9fe7-71d6d9178793 | 85197ff4-006e-46e3-afa1-7735b572f3d0 | potassium | 154.0 | mmol/L | 2024-02-05T13:00:00Z | retracted | 2026-10-14T20:09:54Z |
| beta | ecab3301-bc8f-4d29-adea-94930658663a | aabc25fa-3fe1-4e47-ae9b-ec3635c7936c | sodium | 171.0 | mmol/L | 2024-02-01T10:00:00Z |  | 2026-10-14T20:09:54Z |
| beta | eededb07-e623-4689-9d59-cd2a4eea04e7 | e21342b0-f1ee-4ba3-9343-2e611ca3c448 | glucose | 21.0 | mg/dL | 2024-02-02T12:00:00Z | preliminary | 2026-10-14T20:09:54Z |
| beta | f53d4330-cdda-44ba-ad06-e8cf3805f907 | d98868dd-9c7c-4377-b9a2-8903fbe33b24 | sodium | 266.0 | mmol/L | 2024-02-01T11:00:00Z |  | 2026-10-14T20:09:54Z |
| beta | l-0001 | b-0001 | glucose | 101.0 | mg/dL | 2024-01-05T09:00:00Z | final | 2026-10-14T20:09:54Z |

### silver/clean_providers.csv

| customer | provider_id | provider_name | specialty | active_flag | partition_timestamp |
|---|---|---|---|---|---|
| alpha | 1 | Dr. Ada Synthetic | Cardiology | true | 2026-10-14T20:09:54Z |
| alpha | 2 | Dr. Ben Example |  | true | 2026-10-14T20:09:54Z |
| alpha | 200 | Dr. Bulk Alpha 0 | Family Medicine | true | 2026-10-14T20:09:54Z |
| alpha | 201 | Dr. Bulk Alpha 1 | Family Medicine | true | 2026-10-14T20:09:54Z |
| alpha | 202 | Dr. Bulk Alpha 2 | Emergency | true | 2026-10-14T20:09:54Z |
| alpha | 203 | Dr. Bulk Alpha 3 | Internal Medicine | true | 2026-10-14T20:09:54Z |
| alpha | 3 | Dr. Cy Sample | Neurology | false | 2026-10-14T20:09:54Z |
| alpha | 4 | Dr. Flo Invented |  |  | 2026-10-14T20:09:54Z |
| alpha | 5 | Dr. Gus Madeup |  | true | 2026-10-14T20:09:54Z |
| alpha | 6 | Dr. Hal Imaginary | Nephrology | false | 2026-10-14T20:09:54Z |
| alpha | 7 | Dr. Ivy Pretend | Dermatology | true | 2026-10-14T20:09:54Z |
| beta | 1 | Dr. Dee Placeholder | Oncology | false | 2026-10-14T20:09:54Z |
| beta | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | Dr. Bulk Beta 2 | Emergency | true | 2026-10-14T20:09:54Z |
| beta | 944528c0-0ef8-42d6-b7fd-564637bb3eec | Dr. Bulk Beta 3 |  | true | 2026-10-14T20:09:54Z |
| beta | b7e99aca-a970-45e1-8e46-d534c88a618e | Dr. Bulk Beta 1 | Internal Medicine | true | 2026-10-14T20:09:54Z |
| beta | c0ffee00-0000-4000-8000-0000000000d1 | Dr. Eve Fictional | Radiology |  | 2026-10-14T20:09:54Z |
| beta | fed4057d-bb02-4576-b512-c4c3b253d218 | Dr. Bulk Beta 0 | Emergency | true | 2026-10-14T20:09:54Z |

### gold/provider_daily_activity.csv

| customer | provider_id | encounter_date | provider_name | specialty | active_flag | encounter_count | unique_patients | totaldurationdays | lab_count |
|---|---|---|---|---|---|---|---|---|---|
| alpha | 1 | 2024-01-05 | Dr. Ada Synthetic | Cardiology | true | 2 | 2 | 0.75 | 3 |
| alpha | 1 | 2024-01-06 | Dr. Ada Synthetic | Cardiology | true | 1 | 1 | 1.0 | 1 |
| alpha | 2 | 2024-01-06 | Dr. Ben Example |  | true | 1 | 1 | 1.0 | 1 |
| alpha | 2 | 2024-01-07 | Dr. Ben Example |  | true | 1 | 1 | 0.5 | 1 |
| alpha | 2 | 2024-01-08 | Dr. Ben Example |  | true | 2 | 1 | 0.0 | 0 |
| alpha | 200 | 2024-02-01 | Dr. Bulk Alpha 0 | Family Medicine | true | 1 | 1 | 0.25 | 3 |
| alpha | 200 | 2024-02-04 | Dr. Bulk Alpha 0 | Family Medicine | true | 1 | 1 | 1.0 | 0 |
| alpha | 200 | 2024-02-05 | Dr. Bulk Alpha 0 | Family Medicine | true | 1 | 1 | 0.25 | 2 |
| alpha | 201 | 2024-02-01 | Dr. Bulk Alpha 1 | Family Medicine | true | 2 | 2 | 0.375 | 3 |
| alpha | 201 | 2024-02-02 | Dr. Bulk Alpha 1 | Family Medicine | true | 3 | 2 | 0.75 | 5 |
| alpha | 201 | 2024-02-03 | Dr. Bulk Alpha 1 | Family Medicine | true | 1 | 1 | 1.0 | 1 |
| alpha | 201 | 2024-02-05 | Dr. Bulk Alpha 1 | Family Medicine | true | 1 | 1 | 0.5 | 1 |
| alpha | 202 | 2024-02-01 | Dr. Bulk Alpha 2 | Emergency | true | 1 | 1 | 0.25 | 0 |
| alpha | 202 | 2024-02-02 | Dr. Bulk Alpha 2 | Emergency | true | 2 | 2 | 1.125 | 4 |
| alpha | 202 | 2024-02-04 | Dr. Bulk Alpha 2 | Emergency | true | 1 | 1 | 1.0 | 0 |
| alpha | 202 | 2024-02-05 | Dr. Bulk Alpha 2 | Emergency | true | 1 | 1 | 0.25 | 2 |
| alpha | 203 | 2024-02-01 | Dr. Bulk Alpha 3 | Internal Medicine | true | 2 | 2 | 0.75 | 0 |
| alpha | 203 | 2024-02-02 | Dr. Bulk Alpha 3 | Internal Medicine | true | 2 | 2 | 0.625 | 4 |
| alpha | 203 | 2024-02-04 | Dr. Bulk Alpha 3 | Internal Medicine | true | 1 | 1 | 0.125 | 3 |
| alpha | 203 | 2024-02-05 | Dr. Bulk Alpha 3 | Internal Medicine | true | 1 | 1 | 0.25 | 3 |
| alpha | 3 | 2024-01-09 | Dr. Cy Sample | Neurology | false | 2 | 1 | 0.25 | 4 |
| alpha | 99 | 2024-01-05 |  |  |  | 1 | 1 | 0.25 | 1 |
| beta | 1 | 2024-01-05 | Dr. Dee Placeholder | Oncology | false | 1 | 1 | 0.25 | 1 |
| beta | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | 2024-02-01 | Dr. Bulk Beta 2 | Emergency | true | 2 | 2 | 0.5 | 3 |
| beta | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | 2024-02-03 | Dr. Bulk Beta 2 | Emergency | true | 1 | 1 | 0.5 | 0 |
| beta | 4bf50b52-309d-458c-a7a0-c3d77c967f79 | 2024-02-05 | Dr. Bulk Beta 2 | Emergency | true | 2 | 2 | 0.375 | 0 |
| beta | 944528c0-0ef8-42d6-b7fd-564637bb3eec | 2024-02-01 | Dr. Bulk Beta 3 |  | true | 1 | 1 | 0.5 | 1 |
| beta | 944528c0-0ef8-42d6-b7fd-564637bb3eec | 2024-02-03 | Dr. Bulk Beta 3 |  | true | 1 | 1 | 0.5 | 0 |
| beta | 944528c0-0ef8-42d6-b7fd-564637bb3eec | 2024-02-04 | Dr. Bulk Beta 3 |  | true | 1 | 1 | 0.125 | 1 |
| beta | b7e99aca-a970-45e1-8e46-d534c88a618e | 2024-02-01 | Dr. Bulk Beta 1 | Internal Medicine | true | 4 | 4 | 2.0 | 5 |
| beta | b7e99aca-a970-45e1-8e46-d534c88a618e | 2024-02-04 | Dr. Bulk Beta 1 | Internal Medicine | true | 1 | 1 | 0.125 | 3 |
| beta | b7e99aca-a970-45e1-8e46-d534c88a618e | 2024-02-05 | Dr. Bulk Beta 1 | Internal Medicine | true | 1 | 1 | 0.5 | 2 |
| beta | c0ffee00-0000-4000-8000-0000000000d1 | 2024-01-06 | Dr. Eve Fictional | Radiology |  | 1 | 1 | 0.5 | 0 |
| beta | fed4057d-bb02-4576-b512-c4c3b253d218 | 2024-02-01 | Dr. Bulk Beta 0 | Emergency | true | 1 | 1 | 0.25 | 0 |
| beta | fed4057d-bb02-4576-b512-c4c3b253d218 | 2024-02-02 | Dr. Bulk Beta 0 | Emergency | true | 1 | 1 | 0.25 | 3 |
| beta | fed4057d-bb02-4576-b512-c4c3b253d218 | 2024-02-04 | Dr. Bulk Beta 0 | Emergency | true | 2 | 2 | 0.25 | 2 |
