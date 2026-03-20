# Introduction
## Topic
We analyzed a slice of the internet by parsing around 27 million URLs with the goal of investigating the influence of AI on the web.
## Research Questions
- AI Influence
    - What word usage differs the most between AI and non-AI content?
    - How much do AI domains cite other AI domains and how do non-AI domains compare?
    - How much do AI generated news articles refer to specific categories of sites compared to non-AI articles?
- AI Topic Terminology
    - What words most strongly distinguish AI topic pages from non-AI topic pages?
    - What percentage of each site type uses AI buzzwords?
- Metadata 
    - How much do wikis get cited over other sources?
    - What is the composition in terms of AI generation, AI topic, and site type?

## Data
We initially crawled a lot of sites ourselves, however as we minified the HTML (meaning we took out pretty much everything to save space) we figured out that we can't have enough research questions based on the data we had left. Since we were two weeks into the project already, there was no option to redo the literal months of crawling we had previously done, so we used a dataset from Commoncrawl, which contained raw HTML similar to what we had before we minified it. We worked on this raw HTML, parsing it down to the essentials (similar to Firefox's reading mode, if you've ever used that) and then did our analysis on this simplified HTML.

# Data Pipeline
## Data Acquisition 
We crawled data, which we latter supplemented with CommonCrawl data, both containing pure HTTP responses. We also used NewsAPI to gather domains of news sites. Finally, while it is uncertain if this specifically counts as data, one of our sources was the HuggingFace model `fakespot-ai/roberta-base-ai-text-detection-v1`. 

## Filtering
We analyzed the language using `fast_langdetect` and only kept English sites, as our AI detection model is significantly more consistent with English data. We also only kept sites that could be parsed via `LexborHTMLParser`

## Enrichment
We applied `simplemma` to lemmatize terms, meaning we changed them to their base form. For instance, running and ran would become run, to normalize the data. Afterwards we applied AI topic detection via a weighted list of curated buzzword, which we applied onto our data. We then classified the data based on a very carefully set threshold. 

We extracted 3 key features from the filtered data. First we ran our text data through our AI Text Classification model to label each site with "AI", "HUMAN", "UNSURE". For the purposes of our analysis, we decided to only classify data with "AI" or "HUMAN" when we have very high confidence (95%).

We also looked at all urls to determine whether a site is a shop, news, blog or wiki site by checking for substrings such as "shop." or "/news/". We also supplemented the classification using a known list of news sites from NewsAPI.org. 

# Website
We built the Website using Astro, Tailwind, and the libraries D3 and Plot by Observable. It was deployed on our own VPS. The data for the visualization is supplied in CSV files, provided by a Jupyter notebook in which we did the analysis.

## Using the Website
Scroll down, click some checkboxes, change some selects, and hover over some stuff. Also our process is in the navbar under "Process". The main data is directly on the main page. Some highlights include our self-made UI library providing Tabs and Popovers, a generally mobile-friendly design including a navbar which moves to a more appropriate place on mobile and there's also a picture of Weird Al if you can find it ;3 

On our extraordinarily resplendent website, you will be granted the privilege to observe, with thine own mortal eyes, several extremely interesting inclusions such as AI-heavy domains tending to cite other AI-heavy domains than other domains, 7% of sites likely being AI-generated, and that, compared to news, blogs talk almost twice as much about AI topics.

# AI-assistance
The graphs, specifically the visualization, on the website were AI-assisted as we struggled with the D3 library, however the data was handmade, as was the rest of the website. In other words, the LLM *only* helped with dealing with D3, it did not do any data processing for us. The AI-assisted files are in `./website/data-science-website/src/components/Graphs/`. Everything else was also handmade, no vibe coding involved.
