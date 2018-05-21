

# load libraries ----
library(tidyverse)
library(jsonlite)
library(lubridate)
library(ggforce)
library(data.table)
library(fuzzyjoin)

# define functions ----
get_schedule <- function(season = "20172018") {
  fromJSON(paste0(
    "http://live.nhl.com/GameData/SeasonSchedule-",
    season ,
    ".json"
  )) %>%
    mutate(est = as_datetime(est),
           date = as.Date(est))
}
# tester 8468309
get_player_data <- function(id = "8475172") {
  #Nazem Kadri
  print(id)
  tmp <-
    fromJSON(paste0("https://statsapi.web.nhl.com/api/v1/people/", id , ".json")) %>% .[["people"]]
  cn <- colnames(tmp)
  if ("currentTeam"  %in% cn) {
    # currentTeam is a nested data.frame, unnest and bind
    # some players no longer have a team,
    # so they don't have the currentTeam nested data.frame
    cT <- tmp$currentTeam
    colnames(cT) <- paste0("currentTeam.", colnames(cT))
    tmp2 <-
      bind_cols(tmp %>%  select(-currentTeam,-primaryPosition), cT)
  } else{
    tmp2 <- tmp %>%  select(-primaryPosition)
  }
  # primaryPosition is a nested data.frame, unnest and bind
  pP <- tmp$primaryPosition
  colnames(pP) <- paste0("primaryPosition.", colnames(pP))
  tmp3 <- bind_cols(tmp2, pP)
  colnames(tmp3) <- paste0("player.", colnames(tmp3))
  tmp3
}

get_data  <- function(gameid) {
  print(gameid)
  fromJSON(paste0(
    "http://statsapi.web.nhl.com/api/v1/game/",
    gameid,
    "/feed/live"
  ))
}

process_data  <- function(pbp) {
  print(pbp$gamePk)
  # lots of nested data frames in the play-by-play data, we'll need to unnest and bind.
  # Some games such as "2017020018" are broken and have no play by play data.
  # skip the tibble creating part as required.
  #https://www.nhl.com/gamecenter/tbl-vs-fla/2017/10/07/2017020018#game=2017020018,game_state=final,game_tab=plays
  if (length(pbp$liveData$plays$allPlays) > 0) {
    allPlays <- pbp$liveData$plays$allPlays
    results <-
      pbp$liveData$plays$allPlays$result %>% select(-strength)
    strength <-
      pbp$liveData$plays$allPlays$result$strength  # strength is a nested data.frame in result
    about <- pbp$liveData$plays$allPlays$about %>% select(-goals)
    goals <-
      pbp$liveData$plays$allPlays$about$goals # goals is a nested data.frame in about
    coordinates <- pbp$liveData$plays$allPlays$coordinates
    
    team <- pbp$liveData$plays$allPlays$team
    colnames(strength) <- paste0("strength.", colnames(strength))
    colnames(goals) <- paste0("goals.", colnames(goals))
    colnames(team) <- paste0("team.", colnames(team))
    
    # create events data frame
    mydf_events <- bind_cols(about , goals,
                             results , strength,
                             coordinates,
                             team) %>%
      mutate(gamePk = pbp$gamePk) %>%
      select(gamePk, eventId, eventIdx, everything()) %>% as_tibble()
    
    # create events_players data frame
    players <- pbp$liveData$plays$allPlays$players
    # players is a list  has two nested data frames for each player, $player and $playerType, bind them
    players <-
      players %>% map(function(x)
        bind_cols(as_tibble(x$player), as_tibble(x$playerType)))
    
    z <- map(players, function(x) {
      if (length(colnames(x)) > 0) {
        colnames(x) <- paste0("player.", colnames(x))
      }
      x
    })
    
    # add the gamePk (game id) and eventId to the player event data
    l <- list(
      players = z,
      eventId = about$eventId,
      eventIdx = about$eventIdx,
      gamePk = pbp$gamePk
    )
    mydf_events_players <-
      pmap_df(l, function(players, eventId, eventIdx, gamePk)
        players  %>%
          mutate(
            eventId = eventId,
            eventIdx = eventIdx,
            gamePk = gamePk
          )) %>%
      select(gamePk, eventId, eventIdx, everything())
    #return ta list of two tibbles, one for events and one for events_players
    list(events = mydf_events, events_players = mydf_events_players)
  } else {
    #if no play by play data, return a list of empty tibbles
    list(events = tibble(), events_players = tibble())
  }
}

# Example - single game use ----
schedule <- get_schedule(season = "20172018")
single_game_data <- get_data("2017030321")
single_game_processed <- process_data(single_game_data)
events <- single_game_processed$events
events_players <- single_game_processed$events
single_players_data <- get_player_data("8475172")

# Example - purrr the whole season----

# 1 - get game schedule ----

schedule <- get_schedule()  %>% filter(est < Sys.time() - 12 * 60 * 60)
# keep games that started at least 12 hours ago, because games in progress have
# events, but don't have the "strength" nested data.frame

# For each game ID in the schedule, we will download the play by play data ,
# in list format (pbp) then wrangle the data to create two tables:
# one containing the events (one row per game_id + eventId)and one containing the events_players (one row per game_id + eventId + player_id)

# 2 - download data from past games ----
game_data <- schedule %>%  mutate(pbp = map(id, get_data))
saveRDS(game_data, "game_data.rds")
#

# 3 - process data ----
game_data <- readRDS("game_data.rds")
# 3A process with purrr:map ----
processed_data <- game_data %>%
  mutate(processed = map(pbp , process_data)) %>%
  mutate(events = map(processed, ~ .x[["events"]]),
         events_players = map(processed, ~ .x[["events_players"]]))

mydf_events <-  processed_data$events %>% bind_rows()
mydf_events_players <- processed_data$events_players %>% bind_rows()
saveRDS(processed_data, "processed_data.rds")
saveRDS(mydf_events, "mydf_events.rds")
saveRDS(mydf_events_players, "mydf_events_players.rds")

# 3B alternative process with parallel::parLapply ----

library(parallel)
cl <- makeCluster(parallel:::detectCores() - 1)
clusterEvalQ(cl, library("tidyverse"))
clusterExport(cl, "game_data")
processed_data_parallel_tmp <-
  parLapply(cl = cl, X = game_data$pbp, fun = process_data)
stopCluster(cl)
processed_data_parallel <-
  processed_data_parallel_tmp %>%  transpose %>% as_tibble()
saveRDS(processed_data_parallel, "processed_data_parallel.rds")

mydf_events_parallel <-
  processed_data_parallel$events %>% bind_rows()
mydf_events_players_parallel <-
  processed_data_parallel$events_players %>% bind_rows()

# 4 get data for players involved in any game ----
players_data <- mydf_events_players %>%
  distinct(player.id) %>%
  pull(player.id) %>%
  sort() %>%
  map_df(get_player_data)
saveRDS(players_data, "players_data.rds")

# 5 plot data ----

processed_data <- readRDS("processed_data.rds")
mydf_events <- readRDS("mydf_events.rds")
mydf_events_players <- readRDS("mydf_events_players.rds")
players_data <- readRDS("players_data.rds")
#note :blocked shots are awarded to the blocking team, not the shooting team.
# coordinates of blocked shots appear to refer to the location of the block, not of the shot.
# i say this because the coordinates for defensemen shooter are right in front of the net.
shots <-
  mydf_events %>% filter(event %in% c("Goal", "Shot", "Missed Shot")) %>% #, "Blocked Shot"
  filter(!is.na(x)) %>%
  mutate(
    y = case_when(x < 0 ~ -y, TRUE ~ y),
    #rotate shots so that everything is in the same end of the rink.
    x = abs(x),
    success = ifelse(event == "Goal", 1, 0),
    angle = atan(y / (89 - x)) * 180 / pi,
    distance = ((89 - x) ^ 2 + (y ^ 2)) ^ 0.5
  ) %>%
  left_join(mydf_events_players %>% select(gamePk, eventId, eventIdx, player.id, player.value)) %>%
  left_join(players_data) %>% filter(player.value %in% c("Shooter", "Scorer"))

misses <- mydf_events %>% filter(event %in% c("Missed Shot")) %>%
  filter(!is.na(x)) %>%
  mutate(
    y = case_when(x < 0 ~ -y, TRUE ~ y),
    #rotate shots so that everything is in the same end of the rink.
    x = abs(x),
    success = ifelse(event == "Goal", 1, 0),
    angle = atan(y / (89 - x)) * 180 / pi,
    distance = ((89 - x) ^ 2 + (y ^ 2)) ^ 0.5
  ) %>%
  left_join(mydf_events_players %>% select(gamePk, eventId, eventIdx, player.id, player.value)) %>%
  left_join(players_data) %>% filter(player.value %in% c("Shooter", "Scorer"))


blocks <- mydf_events %>% filter(event %in% c("Blocked Shot")) %>%
  filter(!is.na(x)) %>%
  mutate(
    y = case_when(x < 0 ~ -y, TRUE ~ y),
    #rotate shots so that everything is in the same end of the rink.
    x = abs(x),
    success = ifelse(event == "Goal", 1, 0),
    angle = atan(y / (89 - x)) * 180 / pi,
    distance = ((89 - x) ^ 2 + (y ^ 2)) ^ 0.5
  ) %>%
  left_join(mydf_events_players %>% select(gamePk, eventId, eventIdx, player.id, player.value)) %>%
  left_join(players_data) %>% filter(player.value %in% c("Shooter", "Scorer"))

hits <- mydf_events %>% filter(event %in% c("Hit")) %>%
  filter(!is.na(x)) %>%
  mutate(
    y = case_when(x < 0 ~ -y, TRUE ~ y),
    x = abs(x),
    success = ifelse(event == "Goal", 1, 0),
    angle = atan(y / (89 - x)) * 180 / pi,
    distance = ((89 - x) ^ 2 + (y ^ 2)) ^ 0.5
  ) %>%
  left_join(mydf_events_players %>% select(gamePk, eventId, eventIdx, player.id, player.value)) %>%
  left_join(players_data) %>% filter(player.value %in% c("Hitter"))

# ice features
faceoff_circles <- data.frame(
  x0 = c(0, 69, 69, 20, 20),
  y0 = c(0, 22, -22, 22, -22),
  r = c(15, 15, 15, 2, 2)
) #neutral should be 1, but too small for my liking

goal_rectangle <- data.frame(
  xmin = 89,
  xmax = 91,
  ymin = -3,
  ymax = 3
)

blue_line <- data.frame(x = c(25, 25),
                        y = c(-40, 40))

goal_line <- data.frame(x = c(89, 89),
                        y = c(-40, 40))



#hexbins des shots
ggplot(shots  %>% filter(player.primaryPosition.code != "G")) +
  geom_hex(aes(x = x, y = y), bins = 25) +
  geom_circle(
    data = faceoff_circles,
    aes(x0 = x0, y0 = y0, r = r),
    fill = NA,
    color = "white"
  ) +
  geom_rect(
    data = goal_rectangle,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax
    ),
    fill = "white",
    color = "red"
  ) +
  geom_line(data = goal_line, aes(x = x, y = y), color = "red") +
  geom_line(data = blue_line, aes(x = x, y = y), color = "blue") +
  coord_cartesian(xlim = c(0, 100)) +
  coord_fixed(ratio = 1) +
  facet_wrap( ~ player.primaryPosition.code) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  labs(
    title = "Shots origin by position, 2017-2018 season",
    caption = paste0("(based on data from NHL API as of ", Sys.Date(), ")")
  )
ggsave("shots.png")

#mcdavid shots
ggplot(shots  %>% filter(player.fullName == "Connor McDavid")) +
  geom_hex(aes(x = x, y = y), bins = 25) +
  geom_circle(
    data = faceoff_circles,
    aes(x0 = x0, y0 = y0, r = r),
    fill = NA,
    color = "white"
  ) +
  geom_rect(
    data = goal_rectangle,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax
    ),
    fill = "white",
    color = "red"
  ) +
  geom_line(data = goal_line, aes(x = x, y = y), color = "red") +
  geom_line(data = blue_line, aes(x = x, y = y), color = "blue") +
  coord_cartesian(xlim = c(0, 100)) +
  coord_fixed(ratio = 1) +
  facet_wrap( ~ player.primaryPosition.code) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  labs(
    title = "Connor McDavid shots by location, 2017-2018 season",
    caption = paste0("(based on data from NHL API as of ", Sys.Date(), ")")
  )
ggsave("mcdavid_shots.png")

#hexbins des hits
ggplot(hits %>% filter(player.primaryPosition.code != "G" %>% head(1000))) +
  geom_hex(aes(x = x, y = y), bins = 25) +
  geom_circle(
    data = faceoff_circles,
    aes(x0 = x0, y0 = y0, r = r),
    fill = NA,
    color = "white"
  ) +
  geom_rect(
    data = goal_rectangle,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax
    ),
    fill = "white",
    color = "red"
  ) +
  geom_line(data = goal_line, aes(x = x, y = y), color = "red") +
  geom_line(data = blue_line, aes(x = x, y = y), color = "blue") +
  coord_cartesian(xlim = c(0, 100)) +
  coord_fixed(ratio = 1) +
  facet_wrap( ~ player.primaryPosition.code) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  labs(
    title = "Hits location by position, 2017-2018 season",
    caption = paste0("(based on data from NHL API as of ", Sys.Date(), ")")
  )

ggsave("hits.png")

#hexbins des goals
ggplot(data = shots %>% filter(event == "Goal") %>% filter(player.primaryPosition.code != "G")) +
  geom_hex(aes(x = x, y = y), bins = 25) +
  geom_circle(
    data = faceoff_circles,
    aes(x0 = x0, y0 = y0, r = r),
    fill = NA,
    color = "white"
  ) +
  geom_rect(
    data = goal_rectangle,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax
    ),
    fill = "white",
    color = "red"
  ) +
  geom_line(data = goal_line, aes(x = x, y = y), color = "red") +
  geom_line(data = blue_line, aes(x = x, y = y), color = "blue") +
  coord_cartesian(xlim = c(0, 100))  +
  coord_fixed(ratio = 1) +
  facet_wrap( ~ player.primaryPosition.code) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  labs(
    title = "Goals location by position, 2017-2018 season",
    caption = paste0("(based on data from NHL API as of ", Sys.Date(), ")")
  )
ggsave("goals.png")


ggplot(data = shots %>% filter(event == "Goal") %>% filter(player.fullName == "Connor McDavid")) +
  geom_hex(aes(x = x, y = y), bins = 25) +
  geom_circle(
    data = faceoff_circles,
    aes(x0 = x0, y0 = y0, r = r),
    fill = NA,
    color = "white"
  ) +
  geom_rect(
    data = goal_rectangle,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax
    ),
    fill = "white",
    color = "red"
  ) +
  geom_line(data = goal_line, aes(x = x, y = y), color = "red") +
  geom_line(data = blue_line, aes(x = x, y = y), color = "blue") +
  coord_cartesian(xlim = c(0, 100))  +
  coord_fixed(ratio = 1) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  labs(
    title = "Connor McDavid Goals by location, 2017-2018 season",
    caption = paste0("(based on data from NHL API as of ", Sys.Date(), ")")
  )
ggsave("mcdavid_goals.png")


# hexplot taux de succès
capped_mean <- function(x) {
  if (length(x) > 10) {
    mean_x = mean(x)
    mean_x #  min(mean_x, 0.25)
  } else{
    NA
  }
}

ggplot() +
  stat_summary_hex(
    data = shots %>% filter(is.na(emptyNet) | emptyNet == FALSE),
    bins = 25,
    aes(x = x, y = y, z = success),
    fun = capped_mean
  ) +
  geom_circle(
    data = faceoff_circles,
    aes(x0 = x0, y0 = y0, r = r),
    fill = NA,
    color = "white"
  ) +
  geom_rect(
    data = goal_rectangle,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax
    ),
    fill = "white",
    color = "red"
  ) +
  geom_line(data = goal_line, aes(x = x, y = y), color = "red") +
  geom_line(data = blue_line, aes(x = x, y = y), color = "blue") +
  coord_cartesian(xlim = c(0, 100)) +
  coord_fixed(ratio = 1) +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank()) +
  labs(
    title = "Shot efficiency by location, 2017-2018 season",
    caption = paste0(
      "(minimum of 10 shots per hex, based on data from NHL API as of ",
      Sys.Date(),
      ")"
    )
  )
ggsave("goal_pct.png")



# 6 - model data ----

# 7 - mess around with shift data -----

players_data <- readRDS("players_data.rds")
game_data <- readRDS("game_data.rds")
events <- readRDS("mydf_events.rds")
events_players <- readRDS("mydf_events_players.rds")

pbp <- fread("pbp_20172018.csv") %>% as_tibble()
game2017020001 <- pbp %>% filter(game_id == "2017020001")
mygame2017020001 <- events %>% filter(gamePk == "2017020001")


shifts <-
  fromJSON("http://www.nhl.com/stats/rest/shiftcharts?cayenneExp=gameId=2017020001")

# shifts_data a une ligne par joueur-shift
shifts_data <- shifts[["data"]]  %>% as_tibble() %>%
  left_join(
    players_data %>% select(playerId = player.id, player.fullName, player.primaryPosition.code)
  ) %>%
  select(
    teamName,
    period,
    startTime,
    endTime,
    shiftNumber,
    lastName,
    player.primaryPosition.code,
    everything()
  ) %>% arrange(teamName, period, startTime)

# skaters prend shifts_data, enlève les gardiens et crée int_start_time et int_end_time
skaters <-
  shifts_data %>% filter(player.primaryPosition.code != "G") %>%
  mutate(
    int_start_time = as.integer(str_sub(startTime, 1, 2)) * 60 +  as.integer(str_sub(startTime, 4, 5)) ,
    int_end_time = as.integer(str_sub(endTime, 1, 2)) * 60 +  as.integer(str_sub(endTime, 4, 5))
  )

#list_time est la lsite de toutes les 3600 secondes du match( 3 périodes * 1200 secondes)
list_time <-
  data.frame(period = c(rep(1, 1200), rep(2, 1200), rep(3, 1200)),
             time = c(rep(seq(1, 1200), 3)))



allseconds <- list_time  %>%
  left_join(
    skaters %>%
      select(
        teamAbbrev,
        period,
        int_start_time,
        int_end_time,
        lastName,
        playerId,
        player.primaryPosition.code
      ) %>%
      filter(int_start_time < int_end_time) %>%
      mutate(data = map2(int_start_time, int_end_time, ~ {
        ## Génération des secondes du shift
        data_frame(time = seq(from = .x + 1,
                              to = .y,
                              by = 1))
      })) %>%
      select(-int_end_time, -int_start_time)  %>%
      unnest(data)
  )


# I used to do a fuzzy join pour voir qui est sur la glace pour chacune des secondes
#https://stackoverflow.com/questions/37289405/dplyr-left-join-by-less-than-greater-than-condition
# but generating all the seconds of the shift + left join is much faster.

# allseconds <- list_time %>% fuzzy_left_join(skaters %>% select(teamAbbrev, periodz = period, int_start_time, int_end_time, lastName,playerId, player.primaryPosition.code),
#                                                by=c("period" = "periodz", "time" = "int_start_time", "time" = "int_end_time"),
#                                                match_fun = list(`==`, `>`, `<=`))
# allsecondsTOR <- list_time %>% fuzzy_left_join(skaters %>% filter(teamAbbrev =="TOR") %>% select(teamAbbrev, periodz = period, int_start_time, int_end_time, lastName,playerId, player.primaryPosition.code),
#                               by=c("period" = "periodz", "time" = "int_start_time", "time" = "int_end_time"),
#                               match_fun = list(`==`, `>`, `<=`))
# allsecondsWPG <- list_time %>% fuzzy_left_join(skaters %>% filter(teamAbbrev =="WPG") %>% select(teamAbbrev, periodz = period, int_start_time, int_end_time, lastName,playerId, player.primaryPosition.code),
#                                                by=c("period" = "periodz", "time" = "int_start_time", "time" = "int_end_time"),
#                                                match_fun = list(`==`, `>`, `<=`))
#

shifts <-
  allseconds %>% select(period, time, teamAbbrev, playerId) %>% arrange(period, time, teamAbbrev, playerId) %>%  nest(-period,-time)  %>%
  mutate (
    team =  map(data, ~ .x[["teamAbbrev"]]),
    players = map(data, ~ .x[["playerId"]]),
    lagplayers = lag(players),
    lagperiod = lag(period),
    #same = map2(players,lagplayers, function(x,y){ identical(x,y)}),
    same = pmap(list(players, lagplayers, period, lagperiod), function(players, lagplayers, period, lagperiod) {
      identical(players, lagplayers) & identical(period, lagperiod)
    }),
    shift =  cumsum(same == FALSE)
  ) %>%
  select(-lagplayers,-data,-lagperiod) %>%
  group_by(shift) %>%
  mutate (int_start_time = min(time),
          ## i'd rather summarise than mutate+filter, but I cant group by players because it is a list
          int_end_time = max(time)) %>%
  ungroup() %>%
  filter(same == FALSE) %>%
  mutate(duration = int_end_time - int_start_time + 1) %>%
  select(-same,-time)


goals <- mygame2017020001 %>% filter(event == "Goal") %>%
  mutate(time = as.integer(str_sub(periodTime, 1, 2)) * 60 +  as.integer(str_sub(periodTime, 4, 5))) %>%
  select(period, time, description, team.triCode)

shifts_n_goals <-
  shifts %>% fuzzy_left_join(
    goals %>% select(
      periodz = period,
      timez = time,
      description,
      team.triCode
    ),
    by = c(
      "period" = "periodz",
      "int_start_time" = "timez",
      "int_end_time" = "timez"
    ),
    match_fun = list(`==`, `<=`, `>=`)
  ) %>%
  select(-periodz,-timez)

shifts_w_goals <- shifts_n_goals %>% filter(!(is.na(team.triCode)))


create2shifts <- shifts_n_goals %>% mutate(
  TORplayers = map2(players, team, function(players, team) {
    players[team == "TOR"]
  }),
  WPGplayers = map2(players, team, function(players, team) {
    players[team == "WPG"]
  }),
  TORgoal = case_when(team.triCode == "TOR" ~ 1, TRUE ~ 0),
  WPGgoal = case_when(team.triCode == "WPG" ~ 1, TRUE ~ 0)
)  %>%
  select(
    shift,
    period,
    int_start_time,
    int_end_time,
    TORplayers,
    WPGplayers,
    TORgoal,
    WPGgoal,
    duration
  )

### TODO: Now I need to take create2shifts and create dummies for each player ids.

TOR_playerIds <-
  skaters %>% filter(teamAbbrev == "TOR") %>% distinct(playerId)
