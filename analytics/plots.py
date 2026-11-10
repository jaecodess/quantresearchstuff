#!/usr/bin/env python3
"""Task 4 charts (ANLY-06) from the AGGREGATED CSVs written by analytics/run_analytics.py.

  python3 analytics/plots.py --run-id <task4_run_id> [--ev evidence/analytics]

Only week-level or group-level tables are read (a few hundred rows); no row-level data is plotted.
Writes evidence/analytics/fig_q1_volume.png, fig_q2_subgroups.png, fig_q3a_news_market.png, fig_q3b_news_auc.png.
"""

import argparse
import csv
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

BLUE, ORANGE = "#2a78d6", "#eb6834"  # categorical slots 1 and 2
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"


def read(path):
    with open(path) as fh:
        return list(csv.DictReader(fh))


def num(v):
    return float(v) if v not in (None, "", "None") else float("nan")


def style(ax, title=None, ylabel=None):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(MUTED)
    ax.tick_params(colors=MUTED, labelsize=9)
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    if title:
        ax.set_title(title, color=INK, fontsize=10, loc="left")
    if ylabel:
        ax.set_ylabel(ylabel, color=MUTED, fontsize=9)


def errbar(ax, xs, rows, prefix, color, label, offset=0.0):
    m = [num(r[prefix + "mean"]) for r in rows]
    lo = [num(r[prefix + "lo"]) for r in rows]
    hi = [num(r[prefix + "hi"]) for r in rows]
    x = [i + offset for i in xs]
    ax.errorbar(
        x,
        m,
        yerr=[[a - b for a, b in zip(m, lo)], [b - a for a, b in zip(m, hi)]],
        fmt="o",
        ms=6,
        color=color,
        ecolor=color,
        elinewidth=2,
        capsize=0,
        label=label,
    )


def fig_q1(ev, run_id):
    weekly = read(os.path.join(ev, f"q1_weekly_{run_id}.csv"))
    splits = [s for s in ("train", "validation", "test") if any(r["split"] == s for r in weekly)]
    names = {"train": "2013-15 train", "validation": "2016 validation", "test": "2017 test"}
    fig, axes = plt.subplots(1, len(splits), figsize=(3.6 * len(splits), 3.6), sharey=True)
    axes = axes if len(splits) > 1 else [axes]
    for ax, s in zip(axes, splits):
        data = [
            [num(r["mean_excess"]) for r in weekly if r["split"] == s and int(r["quintile"]) == q] for q in range(1, 6)
        ]
        n_weeks = len(data[0])
        ax.boxplot(
            data,
            widths=0.5,
            showfliers=False,
            medianprops={"color": BLUE, "linewidth": 2},
            boxprops={"color": MUTED},
            whiskerprops={"color": MUTED},
            capprops={"color": MUTED},
        )
        ax.axhline(0, color=MUTED, linewidth=1, linestyle="--")
        ax.set_xticks(range(1, 6), ["Q1\nquiet", "Q2", "Q3", "Q4", "Q5\nbusy"])
        style(ax, f"{names[s]} ({n_weeks} weeks)", "weekly mean ln(ratio) − week median" if s == splits[0] else None)
    fig.suptitle(
        "Q1. Next-week source price ratio by within-week quintile of relative volume\n"
        "(one box value per week; whiskers 1.5 IQR; outliers hidden)",
        fontsize=10,
        color=INK,
        x=0.01,
        ha="left",
    )
    fig.tight_layout()
    return fig


GROUPS = [
    ("all", "all"),
    ("quarter", "Q1"),
    ("quarter", "Q2"),
    ("quarter", "Q3"),
    ("quarter", "Q4"),
    ("vol_tercile", "vol_low"),
    ("vol_tercile", "vol_mid"),
    ("vol_tercile", "vol_high"),
    ("dispersion_tercile", "disp_low"),
    ("dispersion_tercile", "disp_mid"),
    ("dispersion_tercile", "disp_high"),
]
LABELS = [
    "All weeks",
    "Q1",
    "Q2",
    "Q3",
    "Q4",
    "Low vol",
    "Mid vol",
    "High vol",
    "Low disp.",
    "Mid disp.",
    "High disp.",
]


def fig_q2(ev, run_id):
    rows = read(os.path.join(ev, f"q2_summary_{run_id}.csv"))
    fig, ax = plt.subplots(figsize=(9, 3.8))
    xs = list(range(len(GROUPS)))
    for split, color, label, off in (
        ("validation", BLUE, "2016 validation", -0.15),
        ("test", ORANGE, "2017 test", 0.15),
    ):
        sel = []
        for dim, g in GROUPS:
            r = next((r for r in rows if r["split"] == split and r["dimension"] == dim and r["group"] == g), None)
            sel.append(
                r or {"paired_model_minus_b3_mean": "", "paired_model_minus_b3_lo": "", "paired_model_minus_b3_hi": ""}
            )
        errbar(ax, xs, sel, "paired_model_minus_b3_", color, label, off)
    ax.axhline(0, color=MUTED, linewidth=1, linestyle="--")
    for x in (0.5, 4.5, 7.5):
        ax.axvline(x, color=GRID, linewidth=1)
    ax.set_xticks(xs, LABELS, fontsize=9)
    style(
        ax,
        "Q2. Weekly AUC difference, frozen model minus B3, by subgroup (mean and 95% week-bootstrap interval)",
        "model − B3 weekly AUC",
    )
    ax.text(2.5, ax.get_ylim()[1], "quarter", ha="center", va="top", fontsize=8, color=MUTED)
    ax.text(6, ax.get_ylim()[1], "stock volatility tercile", ha="center", va="top", fontsize=8, color=MUTED)
    ax.text(9, ax.get_ylim()[1], "market dispersion tercile", ha="center", va="top", fontsize=8, color=MUTED)
    ax.legend(frameon=False, fontsize=9, ncol=2, loc="lower right", bbox_to_anchor=(1, 1.06))
    fig.tight_layout()
    return fig


def fig_q3a(ev, run_id):
    rows = [r for r in read(os.path.join(ev, f"q3a_summary_{run_id}.csv")) if r["tercile"] in ("1", "2", "3")]
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.6))
    for ax, (m, title, yl) in zip(
        axes,
        (
            ("dispersion_", "Cross-sectional dispersion", "sd of ln(ratio) across stocks"),
            ("abs_market_ln_", "Market-wide move", "|mean ln(ratio)| across stocks"),
        ),
    ):
        for timing, color, label, off in (
            ("pre_start", BLUE, "pre-start news (known before the week)", -0.1),
            ("same_week", ORANGE, "same-week news (association only)", 0.1),
        ):
            sel = [r for r in rows if r["timing"] == timing]
            errbar(ax, [int(r["tercile"]) for r in sel], sel, m, color, label, off)
        ax.set_xticks([1, 2, 3], ["Low news", "Mid news", "High news"])
        style(ax, title, yl)
    n = sum(int(r["n_weeks"]) for r in rows if r["timing"] == "pre_start")
    axes[0].legend(frameon=False, fontsize=8, loc="upper left")
    fig.suptitle(
        f"Q3a. Outcome-week market measures by abnormal GDELT news tercile ({n} weeks, Jul 2015 - Jun 2017; "
        "mean and 95% interval)",
        fontsize=10,
        color=INK,
        x=0.01,
        ha="left",
    )
    fig.tight_layout()
    return fig


def fig_q3b(ev, run_id):
    rows = [r for r in read(os.path.join(ev, f"q3b_summary_{run_id}.csv")) if r["scope"] == "2016+2017"]
    order = ["news_low", "news_mid", "news_high"]
    rows = sorted(rows, key=lambda r: order.index(r["tercile"]))
    fig, ax = plt.subplots(figsize=(6, 3.6))
    xs = [1, 2, 3]
    errbar(ax, xs, rows, "model_auc_", BLUE, "frozen model", -0.1)
    errbar(ax, xs, rows, "b3_auc_", ORANGE, "B3 (vol_ratio, reversed)", 0.1)
    ax.axhline(0.5, color=MUTED, linewidth=1, linestyle="--")
    ax.set_xticks(xs, [f"{lab}\n{r['n_weeks']} weeks" for lab, r in zip(("Low news", "Mid news", "High news"), rows)])
    style(ax, "Q3b. Mean weekly AUC by pre-start news tercile\n(2016 + Jan-Jun 2017, 95% interval)", "mean weekly AUC")
    ax.legend(frameon=False, fontsize=8, loc="lower left")
    fig.tight_layout()
    return fig


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", required=True)
    p.add_argument(
        "--ev",
        default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "evidence", "analytics"),
    )
    a = p.parse_args(argv)
    figs = {"fig_q1_volume": fig_q1, "fig_q2_subgroups": fig_q2}
    if os.path.exists(os.path.join(a.ev, f"q3a_summary_{a.run_id}.csv")):
        figs.update({"fig_q3a_news_market": fig_q3a, "fig_q3b_news_auc": fig_q3b})
    for name, fn in figs.items():
        fig = fn(a.ev, a.run_id)
        path = os.path.join(a.ev, f"{name}.png")
        fig.savefig(path, dpi=200, facecolor="white")
        plt.close(fig)
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
