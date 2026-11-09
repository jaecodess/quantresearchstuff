# Report section 5 draft: Data analytics (ANLY-07)

Target about 550 words. Numbers marked [EMR] are filled from `evidence/analytics/` after the EMR run. AI assistance helped draft this text and must be declared in Appendix C.

---

## 5. Data analytics

**Approach.** Three questions were answered on the full data. Row-level work ran in Hive or PySpark on the cluster; only week-level tables were brought to the driver, bootstrapped and plotted. Weeks are the unit of analysis, with 95% intervals from 10,000 week resamples. All model results use the stored predictions of the frozen model from section 4; nothing was retrained. Results describe association, not cause, and source-price-ratio differences are signal diagnostics, not returns.

**Q1. Unusual volume (Hive).** Each week, stocks were split into quintiles of relative volume (the last five days' volume against the stock's own 60-day average). We used Hive because this is one window function and one grouped aggregation over the ticker-week table. [EMR: share above median and mean excess ln ratio for Q1 and Q5 per split; Q5 − Q1 interval; one-sentence reading.] (Figure 5.1)

**Q2. Where is the model reliable? (PySpark).** We compared the frozen model with B3 by quarter, by stock-volatility tercile and by market-dispersion tercile, in 2016 and 2017 separately. PySpark was used because it needs per-subgroup weekly AUC with tie-aware ranks, computed by the same evaluation code as section 4. [EMR: overall paired difference (matches section 4), which groups are higher or lower, whether any point the same way in both years, weeks and rows per group.] (Figure 5.2) With about 13 to 17 weeks per group, differences smaller than about 0.03 cannot be told apart from noise.

**Q3. News activity (Hive + PySpark).** GDELT event counts were turned into an abnormal-news measure per week. Pre-start measures use only archives published before the week begins; same-week measures are labelled separately and used for association only. (a) [EMR: dispersion and market move by news tercile, high − low interval, Spearman.] (Figure 5.3) (b) Over the about 76 out-of-sample weeks with news coverage, [EMR: model and B3 AUC by news tercile, pooled and per year.] (Figure 5.4)

**Comparison with Task 3.** [EMR: subgroup results against the headline null result.]

**Limitations and uncertainty.** Subgroups are small (13 to 25 weeks), so most intervals are wide and several comparisons were made, so one interval excluding zero could be chance; we only count patterns that agree in 2016 and 2017. Dispersion terciles use the outcome week and describe conditions, not a usable signal. GDELT covers only July 2015 to June 2017 and counts events worldwide, and its source base changes over time (hence the abnormal, not raw, measure). The price data has survivorship bias (section 2), and the optional GDELT interaction model was not run, because it would score the 2017 test weeks a second time.
