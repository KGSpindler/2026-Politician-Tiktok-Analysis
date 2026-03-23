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
4. RuRomeo Troelsgaardn a python script to collect profile data (total likes, followers) as well as data for all videos posted in the last 6 months.
5. Analyse the data

### Results
...

#### Data
I found 224 profiles in total, with 192 candidate profiles and 32 party profiles (main, local, youth, fan-ish). Of these, only 195 had uploaded videos. I found 4943 videos in total, where candidates accounted for 4192 (84,8%) videos and parties accountes for 751 (15,2%) videos. There are of course accounts I haven't been able to find, but this mainly decreases the validity of the profile-amount-by-parties, and not the general results of the parties, as I have included all the main profiles with the biggest impact.
<br><br>
There are a few problems with some of the profiles.
A few Politicians only have unofficial profiles - I have used these when it is obvious they are run by a social media manager. For Example @mette_insider_offical for Mette Frederiksen or @radikaleeeforrohden for Thomas Rohden. 


#### Use of AI
All main scripts have been written by ChatGPT and/or Claude to save time - and precision in webscraping for tiktok accounts, as well as collecting video metadata. All accounts have been manually reviewed and easy-to-find accounts that weren't found by the script, were added manually. 



