# HIPAA context: full dates in gold

Gold keeps full, real dates (DOB excluded) so doctors can review a patient's health history. This file records why that is allowed and what it requires.

## Does leaving out DOB help? No.
- HIPAA's Safe Harbor method (45 CFR 164.514(b)(2)(i)(C)) counts every part of a date except the year as an identifier when the date relates to a person.
- That rule names admission, discharge and death dates as well as birth date, and it covers any service date. Visit dates and lab dates alone are enough to make the table PHI.

## Does holding PHI break HIPAA? No.
- HIPAA regulates how PHI is handled; it doesn't ban it. A covered entity may use PHI for treatment without the patient's authorization (164.506).
- Sharing with a provider for treatment is also exempt from the "minimum necessary" rule (164.502(b)(2)(i)).
- Doctors reviewing their own patients' history is exactly the case HIPAA allows.

## What you take on in return
Gold now falls under the Security Rule (164.312):
- access limited to the care team
- audit logs of who read what
- integrity controls
- encryption at rest (S3 and EMR) and in transit
- a signed BAA (business associate agreement) with AWS, with EMR used as one of AWS's HIPAA-eligible services

## Date of birth
- Gold drops DOB. It adds no new HIPAA category, since gold is already PHI, but it is a strong re-identifier: DOB, sex and ZIP together single out most US residents. Doctors need age, not the birthday.
- Silver computes `age_at_encounter` from DOB and the service date, and gold carries that instead. Ages over 89 become 90+.
- Patient matching by name and DOB happens in the EHR or app, not in gold.

## Patient ID
- With full dates in gold, the pseudonym cannot make gold de-identified. Its job is to keep the MRN, a direct identifier that links across systems, out of gold.
- HMAC-SHA256 with a secret key held in AWS Secrets Manager or KMS. Never a plain hash: MRNs come from a small range and can be brute-forced.
- Normalize the MRN and prefix the source facility before hashing; deterministic across tables and loads; key version in the column name.
- Doctor lookup: an app that holds the key computes HMAC(MRN) at query time. No crosswalk table is stored.
