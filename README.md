# GA360Recommender | TEAM 39

# Project Abstract: Building an E-commerce Recommender System Using Google Analytics Data

This project aims to develop a recommender system for an e-commerce platform, predicting items users are likely to be interested in, based on item characteristics such as price, category, and other relevant features available in the [Google Analytics Sample](https://console.cloud.google.com/marketplace/product/obfuscated-ga360-data/obfuscated-ga360-data?project=realtime-gan) dataset hosted on BigQuery. The dataset provides 12 months (August 2016 to August 2017) of obfuscated Google Analytics 360 data from the [Google Merchandise Store](https://shop.merch.google/canada) a real ecommerce store that sells Google-branded merchandise, in BigQuery.

## Dataset Overview

The Google Analytics Sample dataset encompasses a wide array of features, spanning user session data, e-commerce transactions, and item interactions. We have about 300,000 dataset and the key features relevant to this project include:

- **Item Details**: Including price, category, and item ID.
- **User Interactions**: Including product views, add-to-cart events, purchases, and session duration.
- **User Details**: Including Unique ID, Continent, country, region and traffic source. 

A success of prediciting the right item will be considered when a recommended item is purchased. 

The full schema of the data is [available here](https://support.google.com/analytics/answer/3437719?hl=en)

## Research Questions

The project will focus on two primary research questions:

1. How can the integration of collaborative filtering techniques be leveraged to enhance the personalization and relevance of item recommendations in an e-commerce setting?
2. How do user-based and item-based collaborative filtering algorithms compare in terms of accurately predicting user preferences within an e-commerce context?

## Method Class

The project will exclusively employ collaborative filtering approaches to predict user preferences and recommend items. By analyzing patterns of user interactions within the extensive Google Analytics Sample dataset, the system will identify and recommend items that users are likely to be interested in. The models will leverage user-item interaction data, such as product views, add-to-cart events, and purchases, to generate personalized recommendations.

### Algorithms for Comparison:

We plan to implement two collaborative filtering algorithms and conduct a comparative analysis of their outcomes. The algorithms under comparison will be the Latent Factor, User-Based (user-user) and Item-Based (item-item) collaborative filtering approaches:

1. **User-Based Collaborative Filtering**: This algorithm will recommend items by identifying similarities between users based on their interaction histories. By finding users with similar preferences and behaviors, the system can recommend items liked by these similar users to the target user. The similarity between users will be calculated using metrics such as Pearson correlation or cosine similarity, based on their interactions with various items.

2. **Item-Based Collaborative Filtering**: This approach focuses on the similarity between items rather than users. The system will recommend items that are similar to those a user has previously interacted with or shown interest in. Similarity between items will be determined based on the pattern of user interactions, meaning items frequently interacted with by the same set of users are considered similar.

3. **ALS-Based Collaborative Filtering**: This method employs matrix factorization to predict a user's item preferences. It decomposes the original utility matrix into user and item matrices to capture latent factors associated with users and items. Recommendations are made by reconstructing the utility matrix from these latent factors, enabling the system to infer user preferences for items they haven't interacted with. ALS optimizes these matrices to minimize the difference between observed and predicted ratings, providing a scalable approach to collaborative filtering.

