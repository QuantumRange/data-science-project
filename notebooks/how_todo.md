- Word Usage of AI and Non-AI Content
  1. calculating how many lemmas are in group "ai_content", "ai_topic" and "topic"
  2. Normalizing the occurrence between 0 and 1 number
  3. grouping by "ai_content" and "lemma" – and aggregate the sum of occurrences
  4. taking the top 10k most used words by max of ai or human
  5. then sorting by the difference and taking the top 500 and bottom 500
- How much do Al domains cite other Al domains and how do non-Al domains compare?
  1. We took all domains with outflow
  2. filtered out all the self-flow
  3. grouped by key_from, key_to and summed up
- How much do Al generated news articles refer to specific categories of sites compared to non-Al articles?
  1. We took all domains with outflow
  2. filtered out all the self-flow
  3. filtered out all domains that arn't news sites
  4. grouped by topic and summed up
- What words most strongly distinguish Al topic pages from non-Al topic pages?
  1. We took the word distrubtion
  2. grouped by "ai_topic" and "stem"
  3. summed up normalized occurrence
  4. took top 10k by max
  5. took top 20 and bottom 20 different stems
- What type of sites use what amount of Al buzzwords?
  1. we took all domains
  2. if over 50% of the domain contains a topic its counted as this topic
  3. grouped by topic
  4. summed up ai_topic and topic_score
- How much do wikis get cited over other sources?
  1. 
- How is the composition in terms of Al generation, Al topic, and site type?
  1. we grouped by "ai_content", "ai_topic" and "topic"
  2. summed up the count