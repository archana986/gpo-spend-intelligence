# Genie space definition notes

Use `genie_space.serialized.json` to create a Genie Agent over the Gold views in your catalog and schema. Then pass that space ID as `genie_space_id`.

Point every table identifier in the JSON at `<catalog>.<schema>.*` before you import it. The checked-in file uses `main.gpo_spend_intelligence` as an example.

Suggested questions after gold exists:

1. What is total spend under management in this synthetic book?
2. What is contract coverage?
3. Which members have the most off-contract spend?

Answers come from the generator. They are not customer KPIs.
