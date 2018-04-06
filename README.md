# nhl_play_by_play
mess with the NHL data API

I am super jelly of all the nice data visualizations that show up using NBA data.  

This project aims to make obtaining play-by-play data from the NHL easier, then I will probably do some data viz and modelling for the heck of it.

This project builds on the following:  

  * [NHL API Documentation](https://github.com/dword4/nhlapi)  
  * [Stéphane's Fréchete Fetching Play by Play data v2](https://stephanefrechette.com/fetching-nhl-play-by-play-v2/)  
  * A few reddit posts such as [this one](https://www.reddit.com/r/hockey/comments/5qi4a4/anyone_have_experience_using_the_nhl_json_stats/) and [that one](https://www.reddit.com/r/nhl/comments/4zr5bm/api_for_nhl_stats/).  
  * Coordinates are centered on the center of the ice.  This [wikipedia page](https://en.wikipedia.org/wiki/Ice_hockey_rink) tells me that the rink is 200 feet by 85, with the corners having a 28 feet radius.  The goal line is 11 feet from the back (so 89 feet from the center)  and the blue line 64 feet from the goal line (so 25 feet from the center).   
  * This [USA Hockey page](https://www.usahockeyrulebook.com/page/show/1082185-rule-104-face-off-spots-and-face-off-circles) gives the location of the face off spots and circles    
  * I should spend more time reading this [2010 paper by Ken Krzywicki ](http://www.hockeyanalytics.com/Research_files/SQ-RS0910-Krzywicki.pdf) and this [2018 blog post by Matthew J. Kmiecik](https://mattkmiecik.com/post-Multilevel-Modeling-in-R-with-NHL-Power-Play-Data.html)
  
  
  
![screenshot](/mcdavid_goals.png?raw=true "Screenshot")
![screenshot](/shots.png?raw=true "Screenshot")
![screenshot](/goals.png?raw=true "Screenshot")
![screenshot](/goal_pct.png?raw=true "Screenshot")

Here's the link to the shifts JSON's:
http://www.nhl.com/stats/rest/shiftcharts?cayenneExp=gameId=2017020001
