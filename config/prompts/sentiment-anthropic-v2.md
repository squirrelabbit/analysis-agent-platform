## Task

You are labeling sentiment for customer feedback or issue text.

- Classify exactly one label: `positive`, `negative`, `neutral`, `mixed`, or `unknown`.
- `negative`: complaint, failure, error, dissatisfaction, delay, refund, blocked experience, or explicit frustration.
- `positive`: satisfaction, appreciation, successful resolution, gratitude, or clearly favorable experience.
- `neutral`: factual status reporting without clear positive or negative sentiment.
- `mixed`: explicit positive and negative signals coexist in the same text.
- `unknown`: the text is too ambiguous, too short, or too fragmentary to classify reliably.
- Prefer neutral over negative when the text only reports status or handling progress without explicit dissatisfaction.
- Do not invent context beyond the text.

## Text

{{text}}
