# Report section 5 draft: Data analytics (ANLY-07)

Target about 550 words. All figures are from EMR run 20261110-1000-full-ref (`evidence/analytics/`). AI assistance helped draft this text and must be declared in Appendix C.

---

## 5. Data analytics

**Approach.** We answered three questions on the full data. Row-level work ran in Hive or PySpark on the cluster. Only week-level tables were brought to the driver, bootstrapped and plotted. Weeks are the unit of analysis, and every interval is a 95% interval from 10,000 week resamples. Model results use the stored predictions of the frozen model from section 4; nothing was retrained. All results describe association, not cause. Source-price-ratio differences are signal diagnostics, not returns.

**Q1. Unusual volume (Hive).** Each week we split stocks into five groups by relative volume: the last five days' volume against the stock's own 60-day average. Hive suited this because it is one window function and one grouped aggregation over the ticker-week table. Unusually busy stocks finished above the weekly median slightly less often than unusually quiet ones. Busiest minus quietest group:
- 2013-15: −1.5 percentage points [−3.1, +0.1]
- 2016: −1.0 [−3.5, +1.5]
- 2017: −2.3 [−4.6, +0.1]

The direction is the same in all three periods, but every interval includes zero, so this is at most a weak tendency (Figure 5.1).

**Q2. Where is the model reliable? (PySpark).** We compared the frozen model with B3 by quarter, by stock-volatility tercile and by market-dispersion tercile, separately for 2016 and 2017. PySpark was used because this needs tie-aware weekly AUC within each subgroup, computed by the same evaluation code as section 4. Over all weeks, the paired difference was −0.001 in 2016 and +0.014 in 2017, the section 4 result. No subgroup's interval excluded zero. Some subgroups pointed the same way in both years, such as high-volatility stocks (+0.007 and +0.018), but the differences between groups were far smaller than the intervals (Figure 5.2). So we found no subgroup where the model is reliably better or worse than the simple rule.

**Q3. News activity (Hive and PySpark).** We turned GDELT event counts into a weekly abnormal-news measure. Pre-start measures use only archives dated before the week begins. Same-week measures are labelled separately and are used for association only.
- **(a) Market measures.** Over 95 weeks, weeks in the top news tercile had higher dispersion between stocks than the bottom tercile: +0.0049 [+0.0005, +0.0093] on a typical level of 0.030 to 0.035 (Spearman 0.20). When the news fell inside the outcome week, the market-wide move was also larger: +0.0072 [+0.0021, +0.0126] (Figure 5.3).
- **(b) Model accuracy.** Over the 78 out-of-sample weeks with news coverage, the model's mean weekly AUC was 0.515, 0.499 and 0.491 from low to high news, and every interval included 0.5. The paired difference with B3 also included zero in each tercile, and its sign changed between years (Figure 5.4).

**Comparison with Task 3.** The analytics support the headline result of section 4. The model is slightly ahead of B3 overall in 2017, but no period, stock group or news regime shows a reliable edge. Busy news weeks are more dispersed, yet that extra dispersion did not translate into better ranking by the model.

**Limitations and uncertainty.**
- **Small samples.** Subgroups hold 12 to 32 weeks and many comparisons were made, so one interval excluding zero could be chance. We only count patterns that agree in 2016 and 2017.
- **Outcome-week measures.** Dispersion terciles and same-week news describe conditions during the week, not information available in advance.
- **GDELT coverage.** GDELT covers only July 2015 to June 2017, and the 2017 news terciles hold just 5 to 13 weeks. It counts events worldwide, and its sources change over time, which is why we use an abnormal rather than a raw measure.
- **Survivorship bias.** The price data has survivorship bias (section 2).
- **ANLY-05 not run.** The optional GDELT interaction model was not run, because it would score the 2017 test weeks a second time.
