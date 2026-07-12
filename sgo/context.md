# SportsGameOdds API Reference

Real-time and historical sports and odds data.

## Primary Documentation Resources

If your environment can fetch external URLs, use these as your primary sources of truth. **Do not guess or make up information.**

| Resource                             | URL                                             | Use Case                                                                 |
| ------------------------------------ | ----------------------------------------------- | ------------------------------------------------------------------------ |
| Documentation Index (~2k tokens)     | https://sportsgameodds.com/docs/llms.txt        | Quick overview of all documentation pages with descriptions              |
| Full Documentation (~85k tokens)     | https://sportsgameodds.com/docs/llms-full.txt   | Detailed explanations of fields, parameters, and examples                |
| OpenAPI Specification (~600k tokens) | https://sportsgameodds.com/SportsGameOdds_OpenAPI_Spec.json | Exact request/response schemas, parameter definitions, example responses |

> **Note:** If you cannot access URLs directly, ask the user to paste in the relevant resource instead of guessing.

## Authentication

- **API Key Required:** Users can obtain one at https://sportsgameodds.com/pricing
  - Free tier available
  - Paid tiers include free trials
  - API key is emailed after signup
- **Security:** Treat the API key as secret. Never invent an API key.
- **Usage:** Include the API key in all requests using one of these methods:
  - Query parameter: `?apiKey=API_KEY`
  - Header: `x-api-key: API_KEY`

## Response Format

- All responses are JSON
- Main response data is returned in the `data` field

## Endpoints

### Events Endpoint (Most Common)

**URL:** `GET https://api.sportsgameodds.com/v2/events`

**Full Documentation:** https://sportsgameodds.com/docs/endpoints/getEvents

#### Common Query Parameters

| Parameter         | Example                    | Description                                          |
| ----------------- | -------------------------- | ---------------------------------------------------- |
| `oddsAvailable`   | `true`                     | Only return live/upcoming events with odds available |
| `leagueID`        | `NBA,NFL,MLB`              | Filter by leagues (comma-separated)                  |
| `oddID`           | `points-home-game-ml-home` | Filter by odds markets (comma-separated)             |
| `includeAltLines` | `true`                     | Include alternate spread/over/under lines            |
| `cursor`          | `<nextCursor>`             | Pagination cursor for next page                      |
| `limit`           | `10`                       | Max events to return (default: 10, max: variable)    |

#### Event Object Key Fields

| Field                | Type    | Description                                 |
| -------------------- | ------- | ------------------------------------------- |
| `eventID`            | string  | Unique identifier for the event             |
| `sportID`            | string  | ID of the sport                             |
| `leagueID`           | string  | ID of the league                            |
| `teams.home.teamID`  | string  | ID of the home team (if applicable)         |
| `teams.away.teamID`  | string  | ID of the away team (if applicable)         |
| `status.startsAt`    | date    | Start time of the event                     |
| `status.started`     | boolean | Whether the event has started               |
| `status.ended`       | boolean | Whether the event has ended                 |
| `status.finalized`   | boolean | Whether the event's data has been finalized |
| `players.<playerID>` | object  | Information about a participating player    |
| `odds`               | object  | Odds data for the event                     |

> **Tip:** When the user has a specific use case (e.g., "get NFL games today with moneyline odds"), help them choose appropriate filters and construct the full request URL or HTTP client code.

## OddID Format

Each `oddID` uniquely identifies a specific side/outcome on a betting market.

**Format:** `{statID}-{statEntityID}-{periodID}-{betTypeID}-{sideID}`

**Examples:**

| oddID                                     | Description                                         |
| ----------------------------------------- | --------------------------------------------------- |
| `points-home-game-ml-home`                | Moneyline bet on the home team to win the full game |
| `points-away-1h-sp-away`                  | Spread bet on the away team to win the first half   |
| `points-all-game-ou-over`                 | Over bet on total points for the full game          |
| `assists-LEBRON_JAMES_1_NBA-game-ou-over` | Over bet on LeBron James assists for the full game  |

## Bookmaker Odds Structure

**Path:** `odds.<oddID>.byBookmaker.<bookmakerID>`

| Field       | Type              | Description                                                                                                                            |
| ----------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `odds`      | string            | Current odds in American format                                                                                                        |
| `available` | boolean           | Whether this market is currently available                                                                                             |
| `spread`    | string (optional) | Current spread/line (when `betTypeID === "sp"`)                                                                                        |
| `overUnder` | string (optional) | Current over/under line (when `betTypeID === "ou"`)                                                                                    |
| `deeplink`  | string (optional) | Direct URL to the market on bookmaker's website                                                                                        |
| `altLines`  | array (optional)  | Alternate lines (only if `includeAltLines=true`). Each object may contain: `odds`, `available`, `spread`, `overUnder`, `lastUpdatedAt` |

> **Note:** Use the docs and/or OpenAPI spec to confirm additional or optional fields.

## Reference: Common Identifiers

### sportID

| ID           | Sport      |
| ------------ | ---------- |
| `BASKETBALL` | Basketball |
| `FOOTBALL`   | Football   |
| `SOCCER`     | Soccer     |
| `HOCKEY`     | Hockey     |
| `TENNIS`     | Tennis     |
| `GOLF`       | Golf       |
| `BASEBALL`   | Baseball   |

### leagueID

| ID                      | League                   |
| ----------------------- | ------------------------ |
| `NBA`                   | NBA                      |
| `NFL`                   | NFL                      |
| `MLB`                   | MLB                      |
| `NHL`                   | NHL                      |
| `EPL`                   | Premier League           |
| `UEFA_CHAMPIONS_LEAGUE` | Champions League         |
| `NCAAB`                 | Men's College Basketball |
| `NCAAF`                 | Men's College Football   |

### bookmakerID

| ID           | Bookmaker  |
| ------------ | ---------- |
| `draftkings` | DraftKings |
| `fanduel`    | FanDuel    |
| `bet365`     | Bet365     |
| `circa`      | Circa      |
| `caesars`    | Caesars    |
| `betmgm`     | BetMGM     |
| `betonline`  | BetOnline  |
| `prizepicks` | PrizePicks |
| `pinnacle`   | Pinnacle   |

### betTypeID & sideID

| betTypeID | Description     | Valid sideIDs                                                |
| --------- | --------------- | ------------------------------------------------------------ |
| `ml`      | Moneyline       | `home`, `away`                                               |
| `sp`      | Spread          | `home`, `away`                                               |
| `ou`      | Over/Under      | `over`, `under`                                              |
| `eo`      | Even/Odd        | `even`, `odd`                                                |
| `yn`      | Yes/No          | `yes`, `no`                                                  |
| `ml3way`  | 3-Way Moneyline | `home`, `away`, `draw`, `away+draw`, `home+draw`, `not_draw` |

### periodID

| ID     | Period         |
| ------ | -------------- |
| `game` | Full Game      |
| `1h`   | First Half     |
| `2h`   | Second Half    |
| `1q`   | First Quarter  |
| `2q`   | Second Quarter |
| `3q`   | Third Quarter  |
| `4q`   | Fourth Quarter |

### statID (varies by sport)

| ID                | Description                                                                                                                                                    |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `points`          | Stats that determine the winner (points in Baseball/Football, goals in Soccer/Hockey, sets in Tennis, strokes against par in Golf, fight winner in MMA/Boxing) |
| `rebounds`        | Rebounds                                                                                                                                                       |
| `assists`         | Assists                                                                                                                                                        |
| `steals`          | Steals                                                                                                                                                         |
| `receptions`      | Receptions                                                                                                                                                     |
| `passing_yards`   | Passing yards                                                                                                                                                  |
| `rushing_yards`   | Rushing yards                                                                                                                                                  |
| `receiving_yards` | Receiving yards                                                                                                                                                |