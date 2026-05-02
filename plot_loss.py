#!/usr/bin/env python3
import re, glob, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
OUT_DIR = os.path.join(os.path.dirname(__file__), "plots")
os.makedirs(OUT_DIR, exist_ok=True)

PAT = re.compile(r"Epoch\s+(\d+):\s+expert=([\d.]+)\s+test_ratio=([\d.]+)\s+best=([\d.]+)@ep(\d+)\s+\(target=([\d.]+)\)")

def parse_log(path):
    epochs, loss, ratio, best = [], [], [], []
    target = None
    with open(path) as f:
        for line in f:
            m = PAT.search(line)
            if m:
                epochs.append(int(m.group(1)))
                loss.append(float(m.group(2)))
                ratio.append(float(m.group(3)))
                best.append(float(m.group(4)))
                target = float(m.group(6))
    return epochs, loss, ratio, best, target

logs = sorted(glob.glob(os.path.join(LOG_DIR, "train_*.log")))
runs = []
for p in logs:
    name = os.path.basename(p).replace("train_", "").replace(".log", "")
    e, l, r, b, t = parse_log(p)
    if e:
        runs.append((name, e, l, r, b, t))

print(f"Parsed {len(runs)} runs")

# 1. Per-run figures: expert loss (left) + test ratio (right)
for name, e, l, r, b, t in runs:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4))
    ax1.plot(e, l, "b-o", ms=3)
    ax1.set_xlabel("epoch"); ax1.set_ylabel("expert loss")
    ax1.set_title(f"{name} — expert loss"); ax1.grid(alpha=0.3)

    ax2.plot(e, r, "g-o", ms=3, label="test_ratio")
    ax2.plot(e, b, "r--", lw=1.5, label="best so far")
    if t is not None:
        ax2.axhline(t, color="k", ls=":", lw=1, label=f"target={t}")
    ax2.set_xlabel("epoch"); ax2.set_ylabel("time ratio")
    ax2.set_title(f"{name} — test ratio"); ax2.grid(alpha=0.3); ax2.legend(fontsize=8)

    fig.tight_layout()
    out = os.path.join(OUT_DIR, f"{name}.png")
    fig.savefig(out, dpi=110); plt.close(fig)
    print(f"  -> {out}")

# 2. Combined grid: expert loss for all runs
ncols = 4
nrows = (len(runs) + ncols - 1) // ncols
fig, axes = plt.subplots(nrows, ncols, figsize=(4*ncols, 3*nrows), sharex=True)
axes = axes.flatten()
for ax, (name, e, l, r, b, t) in zip(axes, runs):
    ax.plot(e, l, "b-")
    ax.set_title(name, fontsize=9); ax.grid(alpha=0.3)
    ax.set_ylabel("loss", fontsize=8)
for ax in axes[len(runs):]: ax.axis("off")
fig.suptitle("Expert loss across all 13 runs", y=1.0)
fig.tight_layout()
out = os.path.join(OUT_DIR, "_grid_loss.png")
fig.savefig(out, dpi=110); plt.close(fig)
print(f"  -> {out}")

# 3. Combined grid: test ratio
fig, axes = plt.subplots(nrows, ncols, figsize=(4*ncols, 3*nrows), sharex=True)
axes = axes.flatten()
for ax, (name, e, l, r, b, t) in zip(axes, runs):
    ax.plot(e, r, "g-", label="ratio")
    ax.plot(e, b, "r--", lw=1, label="best")
    if t is not None: ax.axhline(t, color="k", ls=":", lw=1, label=f"target={t}")
    ax.set_title(name, fontsize=9); ax.grid(alpha=0.3)
    ax.legend(fontsize=7)
for ax in axes[len(runs):]: ax.axis("off")
fig.suptitle("Test time-ratio across all 13 runs", y=1.0)
fig.tight_layout()
out = os.path.join(OUT_DIR, "_grid_ratio.png")
fig.savefig(out, dpi=110); plt.close(fig)
print(f"  -> {out}")

# 4. Single overlay: all loss curves together (normalized)
fig, ax = plt.subplots(figsize=(10, 6))
for name, e, l, r, b, t in runs:
    ax.plot(e, l, label=name, lw=1.2)
ax.set_xlabel("epoch"); ax.set_ylabel("expert loss"); ax.set_title("All runs — expert loss")
ax.legend(fontsize=8, ncol=2); ax.grid(alpha=0.3)
fig.tight_layout()
out = os.path.join(OUT_DIR, "_overlay_loss.png")
fig.savefig(out, dpi=110); plt.close(fig)
print(f"  -> {out}")
