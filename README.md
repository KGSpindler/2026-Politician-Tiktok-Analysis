### Analysis of the Danish Folketing Election Candidates tiktok usage over the last 6 months
#### Check out the interactive dashboards [here](https://kgspindler.github.io/kasperspindler/tiktok_election_dashboard.html)
The Danish Folketing election has invigorated politicians to use all means to increase their voters. 
Recently numerous news-channels have reported on politicans usage of Tiktok, especially how Liberal Alliance has used the platform to succesfully increase political reach of the younger generation.
[(1) TV2](https://nyheder.tv2.dk/politik/2026-03-10-tiktok-er-valgets-store-slagmark-men-ikke-alle-partier-lykkes-lige-godt) [(2) DR](https://www.dr.dk/nyheder/politik/har-du-ikke-tiktok-saadan-har-politikerne-foert-valgkamp-derinde)

<br><br>
A few questions arose:
<ul>
	<li>How do the tiktok performance of different candidates and parties compare?
	<ul>
		<li>Which parties get the most views?</li>
		<li>Which parties have the highest views to video ratio?</li>
		<li>Are any non-party-leader candidates specifically succesful?</li>
	</ul>
		<li>How did their behaviour/success change after the election was announced? (26th of February 2026)</li>
	<li>Are there differences in behaviour by voting location?</li>
</ul> 

To answer these questions and more, I set out to explore the Danish Political Landscape on Tiktok by:
1. Find the Folketing Election Candidates
2. Run a python script to search for Candidate Tiktoks
3. Manually review the list, to ensure only actual accounts are included, as well as adding candidate profile that weren't included.
4. Run a python script to collect profile data (total likes, followers) as well as data for all videos posted in the last 6 months.
5. Analyse the data

### Results
There was a 386.5% increase in Tiktok Uploads in the month following the election announcement on the 26th of february compared to the month preceding it. This change in uploading behaviour is especially pronounced for the Moderates (M, 16x) and Alternativet (Å, 45x), who nearly didnt have any activity between the 27th of January and 25th of February. This is opposed to the parties Radikale Venstre (B) and Danish Folkparty (O), who already showed signs of rising acitvity the week before the announcement, maybe having predicted the coming election.

Uploads aggregated by party, over time:
<img width="1349" height="698" alt="image" src="https://github.com/user-attachments/assets/a5926e77-851d-4c95-98e6-de5018182048" />

Prime Minister Mette Frederiksen (A) did not prematurely increase campaign presence, but precisely did upload her first Tiktok video on the announcement date, suggesting a calculated [Tiktok strategy](https://www.dr.dk/nyheder/politik/reels/hvorfor-er-statsministeren-paa-tiktok). 

Liberal Alliance (LA) is the clear leader in Views and Likes, receiving 10 million views and 526k likes during the month preceding the election-day (February 23rd to the 23rd of March). Second place is Danish Folkparty (O) recieved 7 million views and 409k likes whereafter the third, fourth and fifth place is more ambigous. If only looking at views: 3rd is Radical Left (RB, 5.4 mil), 4th is Danish Democrats (DD, 5 mil) fifth is Enhedslisten (Ø, 4.5mil) - but the likes are turned upside down. Enhedslisten recieved 391k likes, Radical Left recieved 346k likes and Danish Democrats recieved 237k likes, showing that while the Danish Democrats and Radical Left recieved more attention and reach with their videos, Enhedslisten recieved more support on tiktok (by likes).

An important thing to notice is how leading figures in the different parties help drive the numbers of each party during the election month. The frontperson of Liberal Alliance, Alex Vanopslagh accounted for 57.9% of all the views of the party, whilst Morten Messerschmidt (O) accounted for 36.1% of his parties views. This is even more pronounced for smaller parties with less members or young candidates like the Danish Democrats where Inger Støjberg accounts for 88.5% of total party views or Lars Løkke Rasmussen in Moderaterne with 64.3% of total party views.

Top 10 candidates by views during election month:
<img width="840" height="417" alt="image" src="https://github.com/user-attachments/assets/06ae3c6f-11b9-490d-9456-8381adc59361" />

The most succesful non-party-leaders are Thomas Rohden (RB) with 1.6 million views, Rosa Lund (Ø) with 1.4 millions views and Romeo Troelsgaard (O) with 1.1 million views.

There are vast geographic differences in uploads, views and likes, but these are, in part, equaled out by famous politicians not running their candidacy in Copenhagen, where they live. This includes Mette Frederiksen, who is running in Northern Jutland and Alex Vanopslagh who is running in East Jutland, and specifically Alex Vanopslagh accounts for around 75% of all views during election month in East Jutland.

Lastly, there is a clear correlation when doing a simple lineær regression between the amount of videos uploaded and amount of views. Using a log10-model I find a coefficient of 0,04235 and suggests that uploading one more video, increases your total views with 10,2%. The correlation is statistically significant even when running simple robustness checks (p=3.31e-19, R^2=0,372). This does of course not mean that more uploads is a better strategy, as there can be omitted variables, or simply that the people who upload a lot have more ressources to make better videos (social media managers e.g.).

<img width="1978" height="1177" alt="image" src="https://github.com/user-attachments/assets/1070ffb3-01d6-4d56-a9da-816d6ec07fa3" />

The election announcement marks a clear turning point in Danish political parties' Tiktok acitivity, with uploads rising sharply immediately afterwards. At the same time the results show that results and visibility on Tiktok is highly uneven accross parties and candidates, with Danish Politics on Tiktok being driven by a few dominant political figures, rather than broad party wide activity. Liberal Alliance stands out as the strongest performer in overall reach, but only on account of the leading figure Alex Vanopslagh, whilst other parties such as Enhedslisten show relatively stronger engagement compared to their view totals.
Overall the findings suggest that Tiktok is becoming an increasingly important, and prioritized tool in the Danish Politicians' campaign toolbox.

#### Data
I found 224 profiles in total, with 192 candidate profiles and 32 party profiles (main, local, youth, fan-ish). Of these, only 195 had uploaded videos. I found 4943 videos in total, where candidates accounted for 4192 (84,8%) videos and parties accountes for 751 (15,2%) videos. There are of course accounts I haven't been able to find, but this mainly decreases the validity of the profile-amount-by-parties, and not the general results of the parties, as I have included all the main profiles with the biggest impact.
<br><br>
There are a few problems with some of the profiles.
A few Politicians only have unofficial profiles - I have used these when it is obvious they are run by a social media manager. For Example @mette_insider_offical for Mette Frederiksen or @radikaleeeforrohden for Thomas Rohden. 
  
Lastly it is clearly possible that candidates have deleted old videos, that might have achieved significant succes on Tiktok. Alex Vanopslagh is one such example, as he was prominent on Tiktok even during the Local Elections in November 2025, but does not have these videos on his account anymore.

#### Use of AI
All main scripts have been written by ChatGPT and/or Claude to save time and increase precision in webscraping for tiktok accounts, as well as collecting video metadata. All accounts have been manually reviewed and easy-to-find accounts that weren't found by the script, were added manually. 



