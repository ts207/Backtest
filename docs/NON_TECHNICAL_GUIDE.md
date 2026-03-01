# A Non-Technical Beginner's Guide to the Backtest Alpha Discovery Platform

Welcome! If you're looking at this project for the first time and wondering, *"What exactly is this, and what does it do?"* — you're in the right place. 

This document explains the Backtest platform in plain English, breaking down its purpose, how it thinks about the market, and how you can use it.

---

## Part 1: The Big Picture

### What is this project?
The Backtest platform is a highly advanced **trading research engine** built for cryptocurrency markets (specifically Binance). 

Its main job is to act like a tireless, mathematically rigorous researcher. You give it years of historical market data, and it scans through billions of data points to find repeating, profitable trading patterns. When it finds them, it double-checks its homework using strict statistics to ensure the patterns are real, not just lucky flukes.

### Why do we need it?
Humans are naturally wired to see patterns, even when they aren't real (like seeing shapes in clouds). If a trader sees the price of Bitcoin drop sharply and bounce back three times, they might invent a rule: *"Buy every time Bitcoin drops sharply."*

However, the human memory easily ignores the 50 other times the price dropped sharply and *didn't* bounce back.

This platform removes human bias. It relies entirely on math, data, and hard facts. It answers questions like: *"If I followed this specific trading rule over the last 5 years, how much money would I have actually made (or lost), accounting for trading fees?"*

---

## Part 2: How the Platform Thinks (Core Concepts)

The platform is designed around three main concepts: **Events**, **States**, and **Rules**.

### 1. Events (The "Triggers")
Instead of constantly looking at the price every second, the engine waits for specific, dramatic moments to happen. These are called **Events**.

Examples of Events this system tracks:
- **`LIQUIDATION_CASCADE`**: A chain reaction where traders are forced to sell, driving the price down rapidly.
- **`VOL_SHOCK`**: The market goes from perfectly calm to extremely chaotic in a matter of minutes.
- **`SPREAD_BLOWOUT`**: The gap between the price buyers are willing to pay and sellers are willing to accept suddenly becomes huge.

The system currently knows how to detect **57 different types of events**. 

### 2. Market States (The "Weather")
An event's effectiveness usually depends on the broader market conditions. A sudden price drop might be a great time to buy during a calm "bull market," but a terrible time to buy during a chaotic "bear market."

The platform tracks the "weather" by labeling every moment in time with a **Market State**.
- Are we in a **high volatility** or **low volatility** regime?
- Are traders mostly betting the price will go up, or down? (Measured by "Funding rates").

### 3. Rules (The "If / Then" Logic)
Once an event happens in a specific state, the system tries to figure out what happens next. It tests different **Rules** across different **Time Horizons** (e.g., waiting 5 minutes vs. waiting 1 hour).

A rule looks like this:
> *IF the `VOL_SHOCK` event happens...*
> *AND the market state is `High Volatility`...*
> *THEN bet that the price will bounce back up (`Mean Reversion`) over the next `15 minutes`.*

---

## Part 3: The Assembly Line (How It Works)

To find these profitable rules, the platform runs a massive "assembly line" divided into distinct stages.

#### Stage 1: Ingest & Clean (Gathering the Ingredients)
The platform downloads years of historical data from Binance. This isn't just price history; it includes order books (who is waiting to buy/sell), trading volume, and market sentiment. It then scrubs this data to fix any errors or gaps.

#### Stage 2: Feature Generation (Prepping the Data)
It calculates complex metrics (like "volatility" or "market depth") and assigns the Market States to every 5-minute window in history.

#### Stage 3: Event Detection (The Scan)
The platform looks at the whole timeline and flags every single instance where one of the 57 Events occurred. It might find that `VOL_SHOCK` happened exactly 312 times in the last 5 years.

#### Stage 4: Discovery (The Interrogation Room)
This is the heart of the system. The platform takes the detected events and tests thousands of "If/Then" Rules against them. 

Because testing thousands of rules means you'll eventually find one that looks profitable purely by blind luck, the platform uses a powerful statistical filter called **FDR Control (False Discovery Rate)** to filter out the noise.

It also stress-tests the ideas through strict **Gates** (Rules it must pass):
- **The Economic Gate & Execution Model:** Does this strategy still make money *after* paying exchange fees and accounting for "slippage" (the hidden cost of placing a large trade)?
- **The Stability Gate:** Did this strategy only work in 2021, and then completely fail in 2023? If it's not stable across the whole timeline, it's thrown out.

#### Stage 5: Strategy Blueprints (The Result)
The surviving ideas—the cream of the crop—are published as **Strategy Blueprints**. These are final, certified "Alpha DNA" recipes that the system believes are genuinely profitable. 

---

## Part 4: The "Spec" System (The Rulebook)

One of the smartest design choices in this platform is that **the rules are separate from the complex engine code.**

Instead of hiding the threshold for a "high volatility" state deep inside thousands of lines of Python code, the project stores these definitions in simple text files called **YAML Specs** (located in the `spec/` folder).

It acts like a configuration dashboard for a non-technical user:
- Want to change what defines a "Liquidation Cascade"? Edit the text file in `spec/events/`.
- Want to require strategies to make a minimum of $10 per trade instead of $5? Change the `gate_economic` threshold in `spec/gates.yaml`.

The engine automatically reads these text files and updates how it behaves. 

---

## Part 5: How You Actually Use It

Even though the engine is doing incredibly complex math, actually running it is designed to be as simple as pressing a button.

The platform is controlled using a command terminal. By typing simple commands (called `make` targets), you tell the engine what to do.

### Example Workflow:

**1. "Just run the discovery engine on Bitcoin and Ethereum from 2020 to 2025."**
You type:
```bash
make discover-blueprints RUN_ID=my_first_experiment SYMBOLS=BTCUSDT,ETHUSDT START=2020-06-01 END=2025-07-10
```
*The orchestrator wakes up, analyzes the 57 events, subjects them to the statistical interrogation room, and generates the Blueprints. (This takes about 30-40 minutes on a standard machine).*

**2. "Show me the top strategies you found."**
You look at the generated `blueprints.jsonl` file, which gives you clear, plain-English outputs like:
> "Event: VOL_SHOCK | Horizon: 15m | Rule: Mean Reversion | Expected Profit Per Trade: 0.05%"

**3. "Now what?"**
Once you have your Blueprints, you import them into the **NautilusTrader** engine to perform high-resolution backtesting and, ultimately, live trading on real markets.

---

## Summary

In short, this project is a "truth machine" for trading ideas. 

It prevents you from betting on flawed human intuition by systematically defining events, testing thousands of combinations against historic data, subjecting them to Ivy-League-level statistics, subtracting the true costs of trading, and giving you only the "Alpha DNA" that survived the gauntlet.
