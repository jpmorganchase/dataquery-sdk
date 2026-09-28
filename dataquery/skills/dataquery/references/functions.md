 # Functions Glossary

> **Grounding:** These are the only 158 functions DataQuery supports. Do not invent, alias, or guess function names or parameters. If a requested analytic has no match here, tell the user rather than approximating. Verify exact syntax with `function-help --name <FUNC>`.

## Aggregate

| Function | Description |
|----------|-------------|
| **AGAVG** | Returns an average of time series. `(Expr1 + Expr2 + ... + ExprN)/N`. If Expr[I] value for a day J is NA, doesn't include value[I,J] in the average. |
| **AGMAX** | Returns a maximum of time series Expr1 to ExprN. If ExprI value for a day J is NA, doesn't include value[I,J] while calculating max. |
| **AGMIN** | Returns a minimum of time series Expr1 to ExprN. If ExprI value for a day J is NA, doesn't include value[I,J] while calculating min. |
| **AGMUL** | Returns a product of time series Expr1 to ExprN. `Expr1 * Expr2 * ... * ExprN`. If Expr[I] value for a day J is NA, doesn't include value[I,J] in the product. |
| **AGNUMVALID** | Calculates a number of valid values of time series 1-N on the given day. |
| **AGSUM** | Returns a sum of time series Expr1 to ExprN. `Expr1 + Expr2 + ... + ExprN`. If Expr[I] value for a day J is NA, doesn't include value[I,J] in the sum. |

## Calendar

| Function | Description |
|----------|-------------|
| **BDINM** | Returns the number of business days in the month of the current query date. |
| **BDINY** | Returns the number of business days in the year of the current query date. |
| **BDTODT** | Returns the number of business days from the current query date to the given date, given in mm/dd/yy format. |
| **DATE** | Returns the current date in the evaluation as a number in yyyymmdd format. |
| **DAY** | Returns the weekday of the current query date (Mon=1, Sun=7). |
| **DOFM** | Returns the day number of the month in the current query date. |
| **DOFY** | Returns the day number of the year in the current query date. |
| **DINM** | Returns the number of days in the current query date's month. |
| **DINY** | Returns the number of days in the current query date's year. |
| **ENDDT** | Returns the end date of the query (as a number in yyyymmdd format) for each day in the query period. |
| **DTODT** | Returns the number of days from the current query date to the given date, given in mm/dd/yy form. |
| **JDAYS** | Without an argument, returns the Julian day offset (days from January 1, 1970) of the current day in the query. You may also specify a date argument, such as a particular date or the results of another function that returns a series of dates. |
| **MONTH** | Returns the month of the current query date. |
| **NBDAYS** | Returns the number of business days between the two given dates. |
| **NDAYS** | Returns the number of days between the two given dates. Syntax: `NDAYS(dt1, dt2)` where both dates are required (mm/dd/yy format). |
| **STARTDT** | Returns the start date of the query (as a number in yyyymmdd format) for each day in the query period. |
| **YEAR** | Returns the year of the current day in the query date range. |

## Futures & Options

| Function | Description |
|----------|-------------|
| **ACCRUED** | Returns accrued interest for the given ETP bond symbol. Accrued is calculated as of regular settlement for the given trade (query) date. *Only works with government bonds from the ETP Database.* |
| **BASIS** | Cash-futures basis. `Basis = BondPrice - FuturesPrice * ConvFactor` |
| **BDTODELIV** | Returns the number of business days from the current query date to delivery of the given futures contract. Optional DelivCode (FIRST or LAST) specifies delivery at beginning or end of the delivery period. Default: end. |
| **BDTOEXPN** | Returns the number of business days to (listed) option expiration. |
| **BDTOLTD** | Returns the number of business days to the last trading day of the given futures contract. |
| **BIMPVOL** | Business day implied volatility for the given options position. Example: `BIMPVOL(ed95z, (ATM CALL, ATM PUT))` |
| **BNOC** | Basis Net of Carry. Optional DelivCode (FIRST or LAST). Default: end. `BNOC = basis + coupon income - financing cost` |
| **BNOCRP** | Basis Net of Carry using a term repo rate to delivery. Optional DelivCode (FIRST or LAST). Default: end. |
| **CALIRP** | Repo rate implied by the spread between two futures positions, or between a cash and a futures position. Optional DelivCode (FIRST or LAST). Default: end. |
| **CASHROLL** | Returns the rolling cash yield or price for hot-run bonds. |
| **DELIV** | Returns the delivery date (yyyymmdd) of the given futures contract. DelivCode may be FIRST or LAST. |
| **DTODELIV** | Returns the number of days from the current query date to delivery of the given futures contract. Specify FIRST or LAST for DelivCode. |
| **DTOEXPN** | Returns the number of days from the current query date to the expiration of the given options contract. |
| **DTOLTD** | Returns the number of days from the current query date to the last trading day of the given futures contract. |
| **DTOMAT** | Returns the number of days from the current query date to the maturity of the given ETP bond. |
| **DV01** | Returns the price value of a basis point for the given ETP bond. (Same as PVBP.) |
| **EXPN** | Returns the expiration date (yyyymmdd) of the given futures options contract. |
| **FACTOR** | Returns the conversion factor for the given deliverable bond and futures contract. *Bond must be an ETP bond symbol.* |
| **FWD** | Returns the forward rate implied by the given ETP yield series. YSeries form: `[ticker]Ynn` (e.g. US, FG + term in years like 02, 05, 10, 30). |
| **FWDPRICE** | Returns the forward price of a bond. BondSymbol is an ETP bond symbol. FwdDateIndicator can be a date (mm/dd/yy), 3B/3D/3M/3Y, or FIRST/LAST. Carry rate indicator: REPO, GC, or LIBOR. |
| **FWSP** | Returns the forward weighted spread (duration weighted difference) between the forward strip yield and the forward yield implied by the given short and long maturity bond. |
| **IMPREPO** | Returns the repo rate implied by the basis between the given ETP bond and futures series. Optional DelivCode (FIRST or LAST). Default: end. |
| **IMPVOL** | Returns the implied volatility for the given options position. Example: `IMPVOL(ed95z, (ATM CALL, ATM PUT))` |
| **IMPVOLF** | Returns implied volatility on an option position for a fixed period (months). Example: `IMPVOLF(2, SERIAL, ft, option 5, (ATM CALL, ATM PUT))`. SERIAL keyword and SwitchCode are optional. |
| **LTD** | Returns the last trading date (yyyymmdd) of the given futures contract. |
| **MATDT** | Returns the maturity date (yyyymmdd) of a bond or T-bill. *Only works on ETP bond or T-bill symbols.* |
| **MODDUR** | Returns the modified duration for the given bond. *Only works for ETP database bond symbols.* |
| **PREMIUM** | Returns the net premium for the given options position. Example: `PREMIUM(ed95z, (ATM CALL))` |
| **PRICE** | Returns the price of the given ETP yield series. |
| **PVBP** | Returns the price value of a basis point for the given ETP bond. (Same as DV01.) |
| **RATE** | Returns rate values and properties given a curve, forward start, and tenor. Curve is defined as `<ccy><sector>`. Available curves include: "usdagency", "usdswap", "usdtreas". Syntax: `RATE(<curve>, ForwardStart, Tenor)`. Example: `RATE(usdtreas, 0Y, 10Y)` |
| **ROLLING** | Returns the price (or data attribute) of the given futures ticker. RollCode: front, back, back2, etc. Optional SwitchCode for roll behavior. Examples: `ROLLING(front, ed)`, `ROLLING(back2, us, option,.iv)` |
| **STOCHASTIC** | Contact ETP Research for details. |
| **STRIKE** | Returns the strike for the given options position. Example: `STRIKE(rolling front ed, (ATM CALL))` |
| **TSYRATE** | Returns swap rates and properties for a given forward start and tenor. Syntax: `TSYRATE(ForwardStart, Tenor)`. Example: `TSYRATE(0Y, 10Y)` |
| **WSP** | Returns the weighted spread (duration weighted difference) between the money market strip and bond yields. |
| **YIELD** | Returns the yield to worst for the given ETP bond series. For T-Bills, specify MMKT or BEY as 2nd argument. Use GADJ flag for Gross Adjusted Yield on Italian government bonds. |
| **YTC** | Yield to 1st call for the given ETP bond series. |
| **YTM** | Yield to maturity for the given ETP bond series. |

## Mathematical

| Function | Description |
|----------|-------------|
| **ABS** | Returns the absolute value of a given expression. |
| **CEIL** | Returns the least integral value >= expression. |
| **EXP** | Returns e (2.71828) raised to a given expression's value. |
| **EXP10** | Returns 10 raised to a given expression's value. |
| **FLOOR** | Returns the greatest integral value <= expression. |
| **LN** | Returns the natural logarithm of a given expression. |
| **LOG** | Returns the natural logarithm of a given expression. |
| **LOG10** | Returns the Base10 logarithm of a given expression. |
| **ROUND** | Returns the value of a given expression rounded to nearest integral value. |
| **SQRT** | Returns the square root for a given expression. |

## Miscellaneous

| Function | Description |
|----------|-------------|
| **BOFM** | Returns the first valid observation of the month for the rest of the month. If range does not include beginning of the month, the rest of the first month values set to NA. |
| **BOFW** | Returns the first valid observation of the week for the rest of week. If range does not include beginning of the week, the rest of the first week values set to NA. |
| **BOFY** | Returns the first valid observation of the year for the rest of year. If range does not include beginning of the year, the rest of the first year values set to NA. |
| **EOFM** | Returns the last valid observation of the month for the rest of the month. If range does not include end of the month, the rest of the last month values set to NA. |
| **EOFW** | Returns the last valid observation of the week for the rest of week. If range does not include end of the week, the rest of the last week values set to NA. |
| **EOFY** | Returns the last valid observation of the year for the rest of year. If range does not include end of the year, the rest of the last (current) year values set to NA. |
| **FALSE** | Returns the number 0, representing logical FALSE. |
| **IF** | If Expr1 is True (not zero), result of Expr2 is returned. If Expr1 is False (zero), result of Expr3 is returned. Arguments are evaluated for each day in the query date range. |
| **ISNA** | Returns True (1) if a given expression is N/A, or False (0) if valid. |
| **ISVALID** | Returns True (1) if a given expression is valid, or False (0) if N/A. |
| **MONTHLY** | Prunes the data into monthly frequency. Frequency conversion is based on the context setting. |
| **NA** | Returns N/A for every day in the query date range. |
| **NAIFZERO** | Returns N/A when a given expression equals zero. |
| **NEXTVALID** | Fill in missing points in the series with the next closest day's value. Will not look forward beyond the end date of the query. |
| **ORDINATE** | Returns the daily offset of each day in the query period. |
| **PERFIRST** | Returns the first valid observation for all days in the range. |
| **PERLAST** | Returns the last valid observation for all days in the range. |
| **PREVVALID** | Returns the most recent valid value for a day when the expression is NA. Will not look back prior to the start date. Optional date parameter for checking last valid value in a given range. Example: `PREVVALID(N10.Y, DATE=[1Y,])` |
| **QUARTERLY** | Prunes the data into quarterly frequency. Frequency conversion is based on the context setting. |
| **RAND** | Returns a random number between 0 and 1. Optional seed parameter. |
| **SIGN** | Returns the sign of a given expression. -1 for negative, 1 for positive, 0 for zero. |
| **SORT** | Returns the results of a given expression as a sorted list of values, placing NAs at the bottom. Results are not related to the dates on which they appear. |
| **TRUE** | Returns the number 1, representing logical TRUE. |
| **WEEKLY** | Prunes the data into weekly frequency. Frequency conversion is based on the context setting. |
| **YEARLY** | Prunes the data into Annual frequency. Frequency conversion is based on the context setting. |
| **ZEROIFNA** | Returns zero if a given expression equals N/A. |

## Statistical

| Function | Description |
|----------|-------------|
| **ADJRSQR** | Returns the adjusted R-squared from the regression of ExprY vs one or more ExprX over the previous NDays. Takes into account degrees of freedom. |
| **ARVOL** | Returns NDays historical arithmetic volatility. `chg = X[n] - X[n-1]`, `mean = (sum of chg)/npoints`, `sumsqr = sum of (chg * chg)`, `vol = SQRT((sumsqr - npoints*mean*mean) / (npoints-1)) * SQRT(251)` |
| **BETA** | Returns the coefficient of the 1st independent variable from linear regression of ExprY vs ExprX over the last NDays. NDays can be NYears (e.g. `1y`), NMonths (`1m`), or NWeeks (`3w`). |
| **CHG** | Returns the change in value of expression from NDays back to current day. Default NDays = 1. |
| **CORR** | Returns the correlation between ExprY and ExprX over the previous NDays. |
| **COVA** | Returns the covariance of ExprY versus ExprX over the previous NDays. |
| **ERF** | Returns the error function of UpperLimit for a normal distribution. Optional Mu, Sigma, LowerLimit. Defaults: Mu=0, Sigma=1/sqrt(2), LowerLimit=0. |
| **ERFC** | Returns the error function complement of UpperLimit for a normal distribution. Optional Mu, Sigma, LowerLimit. Defaults: Mu=0, Sigma=1/sqrt(2), LowerLimit=0. |
| **ESS** | Returns the error sum of squares (residual variation of ExprY) for the given variables. |
| **EWMAAVG** | Returns NDays exponentially weighted moving average. Syntax: `EWMAAVG(NDays, Expr, Factor)`. `EWMAAVG(1) = Y(1)`. For t > 1: `EWMAAVG(t) = Factor * Y(t) + (1 - Factor) * EWMAAVG(t-1)`. Factor is smoothing factor between 0 and 1. |
| **EWMACORR** | Returns NDays exponentially weighted moving average correlation. Syntax: `EWMACORR(NDays, Expr1, Expr2, Factor)`. Factor is a decay factor (0-1). Uses weighted covariance divided by weighted standard deviations. |
| **EWMAVOL** | Returns NDays exponentially weighted moving average volatility. Syntax: `EWMAVOL(NDays, Expr, Factor)`. Factor is a decay factor (0-1). `chg = X[n] - X[n-1]`, `vol = SQRT(weighted_sumsq/accumulated_factor) * SQRT(251) * 100` |
| **FSTAT** | Returns the F-statistic from linear regression of ExprY vs ExprX over the last NDays. Tests significance of R-squared. |
| **INDEX** | Returns the indexed value of a given expression (1st day in date range = 100). |
| **INTERCEPT** | Returns the intercept (constant) from linear regression of ExprY vs ExprX over the last NDays. |
| **IQR** | Returns the interquartile range over the last NDays. |
| **KURTOSIS** | Returns NDays excess kurtosis of a given expression for each day in the query date range. |
| **LAG** | Returns the result of a given expression from NDays ago. Default NDays = 1. |
| **LOGCHG** | Returns the logarithmic change from the previous day: `LOG(Val[x]/Val[x-1])` |
| **LOGNORMCDF** | Returns CDF of a lognormal distribution with Mu, Sigma, optional UpperLimit and LowerLimit. Defaults: UpperLimit=0, LowerLimit=0. |
| **LOGNORMCDFC** | Returns CDF complement of a lognormal distribution. Defaults: UpperLimit=0, LowerLimit=0. |
| **LOGNORMPDF** | Returns PDF of X for a lognormal distribution. Defaults: Mu=0, Sigma=1. |
| **MAX** | Returns the maximum value of a given expression over the last NDays. |
| **MEDIAN** | Returns NDays median of a given expression on each date. For even observations, middle two values are averaged. |
| **MIN** | Returns the minimum value of a given expression over the last NDays. |
| **MODEL** | Returns the linear regression model for ExprY (dependent) vs ExprX1, ExprX2, etc. (independent). |
| **MOVAVG** | Returns NDays average of a given expression on each date. Optional Type (1=sigma trim, 2=winsorized) and Threshold for outlier rejection. |
| **MUL** | Returns the product over the last NDays. If 1st argument missing, returns total product from start date. Returns value if at least 1 valid observation present. |
| **NDEVS** | Returns the number of standard deviations that a given expression deviates from its NDays mean. |
| **NORMCDF** | Returns CDF of a normal distribution. Defaults: UpperLimit=0, LowerLimit=-infinity. |
| **NORMCDFC** | Returns CDF complement of a normal distribution. Defaults: UpperLimit=0, LowerLimit=-infinity. |
| **NORMPDF** | Returns PDF of X for a normal distribution. Defaults: Mu=0, Sigma=1. |
| **OBS** | Returns the observation number for a given expression. Counts only valid observations. Use ORDINATE() for absolute day count. |
| **PCTCHG** | Returns percent change for NDays: `((Val[x]/Val[x-NDays]) - 1) * 100`. Default NDays = 1. |
| **PERAVG** | Returns periodic average over the query date range. Same value returned for all days. |
| **PERCENTILE** | Returns percentile rank. Syntax variants: `PERCENTILE(Expr)` — rank of each observation over entire period. `PERCENTILE(NDays, Expr)` — rank in rolling window. `PERCENTILE(NDays, Expr, Rank)` — value at given percentile rank in rolling window. |
| **PERHIGH** | Returns periodic high over the query date range. Same value for all days. |
| **PERLOW** | Returns periodic low over the query date range. Same value for all days. |
| **PERMEDIAN** | Returns periodic median over the query date range. Same value for all days. |
| **PERSTD** | Returns periodic standard deviation over the query date range. Same value for all days. |
| **PNL** | Returns the change in value from the first day in the query date range. |
| **RANK** | Returns the ordered rank for each observation in a given expression. |
| **RESIDUAL** | Returns residual value from regression. Syntax: `RESIDUAL([NDays], ExprY, ExprX1 [, Expr2,...])`. ExprY is the dependent variable, ExprX1 (and optional Expr2,...) are independent variables. Without NDays: each day in query period. With NDays: last day of each rolling period. NDays can be NYears/NMonths/NWeeks (e.g. `1y`, `1m`, `3w`). |
| **RMS** | Returns NDays Root Mean Square. `sumsqr = sum of X[n]*X[n]`, `RMS = SQRT(sumsqr/npts)` |
| **RMSVOL** | Returns NDays Root Mean Square Volatility. `pctchg = log(X[n]/X[n-1])`, `RMSVOL = SQRT(sumsqr/npts) * SQRT(251) * 100` |
| **RSI** | Computes the Relative Strength Index. Default NDays = 14. |
| **RSQR** | Returns R-squared from regression — proportion of total variation in ExprY explained by regression vs ExprX. |
| **RSS** | Returns regression sum of squares (explained variation of ExprY). |
| **SKEW** | Returns NDays skew of a given expression for each day in the query date range. |
| **SEALPHA** | Returns standard error of the constant term from linear regression of ExprY vs ExprX over last NDays. |
| **SEBETA** | Returns standard error of the 1st independent variable from linear regression over last NDays. |
| **SEREG** | Returns standard error from linear regression of ExprY vs ExprX over last NDays. |
| **STDDEV** | Returns NDays standard deviation. `meansqr = (mean^2)/npts`, `STDDEV = SQRT((sumsqr - meansqr) / (npts - 1))` |
| **SUM** | Returns sum over last NDays. If 1st argument missing, returns running total from start date. Returns value if at least 1 valid observation present. Note: `SUM(NDays, Expr)/NDays` may not equal `MOVAVG(NDays, Expr)`. |
| **TSALPHA** | Returns T-statistic for the constant term from regression. Magnitude >= 2 often indicates statistical significance. Equal to term value / standard error. |
| **TSBETA** | Returns T-statistic for the 1st independent variable from regression. Magnitude >= 2 often indicates significance. Equal to coefficient / standard error. |
| **TSS** | Returns total regression sum of squares (ESS + RSS). |
| **VAR** | Returns NDays variance. `meansqr = (mean^2)/npts`, `VAR = (sumsqr - meansqr) / (npts - 1)` |
| **VOL** | Returns NDays historical volatility. `pctchg = LOG(X[n]/X[n-1])`, `vol = SQRT((sumsqr - npoints*mean*mean) / (npoints-1)) * SQRT(251)` |
| **ZSCORE** | Without NDays: returns zscore for each observation (deviation from mean / stddev). With NDays: zscore of last observation in rolling periods. Multiple expressions supported (uses regression standard error). |

## Usage Patterns

All function expressions are passed to the `expression-timeseries` command, for example:
```bash
dataquery expression-timeseries --expressions "FUNCTION(params, DB(...))" --start-date TODAY-1Y
```

### Function Composition (Nesting)

Functions can be nested to build complex analytics. The innermost expression is always the raw data source (`DB()`), and outer functions transform the result:

```
# 20-day moving average of daily percent changes
MOVAVG(20, PCTCHG(1, DB(BIGI,ABS,Q10,TR,YTDR,LOC)))

# Z-score of 30-day volatility
ZSCORE(252, VOL(30, DB(BIGI,ABS,Q10,TR,YTDR,LOC)))

# Correlation of daily changes between two series
CORR(60, CHG(1, DB(...series1...)), CHG(1, DB(...series2...)))

# Spread between two series (simple arithmetic)
DB(...series1...) - DB(...series2...)

# Moving average of a spread
MOVAVG(20, DB(...series1...) - DB(...series2...))
```

### Aggregate Functions (Cross-Sectional)

Use `AG*` functions to combine multiple time series:

```
# Average of 3 bond yields
AGAVG(DB(...bond1...), DB(...bond2...), DB(...bond3...))

# Max across instruments
AGMAX(DB(...series1...), DB(...series2...), DB(...series3...))

# Sum of returns
AGSUM(DB(...series1...), DB(...series2...))
```

### Frequency Conversion Functions

Use these to request data at a different frequency without the `--frequency` flag:

```
# Monthly closing values
MONTHLY(DB(BIGI,ABS,Q10,TR,YTDR,LOC))

# Weekly data
WEEKLY(DB(FGB,T,0.5,11/15/2034,91282CLW6,MIDYLD))

# Quarterly data
QUARTERLY(DB(...))

# Annual data
YEARLY(DB(...))
```

### NDays Parameter Formats

Many statistical functions accept flexible time-period formats for NDays:
- Integer: business days (for example, `20` = 20 business days)
- `Ny`: years (for example, `1y` = 1 business year)
- `Nm`: months (for example, `3m` = 3 business months)
- `Nw`: weeks (for example, `2w` = 2 business weeks)

### Arithmetic in Expressions

Expressions support basic arithmetic operators between series:
- Addition: `DB(...) + DB(...)`
- Subtraction: `DB(...) - DB(...)` (spreads)
- Multiplication: `DB(...) * DB(...)` or `DB(...) * 100`
- Division: `DB(...) / DB(...)`
- Constants: `DB(...) + 2.5`, `DB(...) * 100`

### Notes on Functions
- Functions execute server-side on the DataQuery engine; no local computation is required.
- Set `--start-date` to account for function lookback. For example, for `VOL(30, ...)` with one year of output, use `--start-date TODAY-1Y`; the engine handles the additional lookback internally.
- Multiple function expressions can be passed in a single call using multiple `--expressions` flags.
- All functions handle NA values gracefully (skipping or propagating, depending on the function).
