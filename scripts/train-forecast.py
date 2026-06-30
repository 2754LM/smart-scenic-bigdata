"""
智能景区大数据平台 - 时序预测模型训练脚本

任务: 根据过去 24 个 30min 窗口的指标，预测未来 8 个 30min 窗口的客流量
- 输入:  data/raw_data/consumption.csv (10w 行消费记录)
- 输出:  data/models/forecast.pt (PyTorch 模型)
         data/models/forecast_meta.json (Scaler 参数 + 训练指标)

使用:
  # 默认 (用 80% 数据训练)
  python scripts/train-forecast.py

  # 用 GPU
  python scripts/train-forecast.py --device cuda

  # 切到 90% 训练
  python scripts/train-forecast.py --train-ratio 0.9

部署到 VM:
  # 把 .pt + .json 复制到 VM
  scp data/models/forecast.pt  user@vm:/opt/scenic/models/
  scp data/models/forecast_meta.json  user@vm:/opt/scenic/models/
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# 延迟 import torch (允许 --help 在没装 torch 的机器上也能用)
def _import_torch(device: str):
    try:
        import torch
        import torch.nn as nn
        import torch.optim as optim
        from torch.utils.data import DataLoader, TensorDataset
    except ImportError:
        print("错误: 需要安装 PyTorch")
        print("  pip install torch --index-url https://download.pytorch.org/whl/cpu")
        sys.exit(1)
    if device == "cuda" and not torch.cuda.is_available():
        print("WARN: CUDA 不可用，回退到 CPU")
        device = "cpu"
    return torch, nn, optim, DataLoader, TensorDataset, device


# ============== 配置 ==============
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_CSV = PROJECT_DIR / "data" / "raw_data" / "consumption.csv"
OUT_DIR = PROJECT_DIR / "data" / "models"
MODEL_PATH = OUT_DIR / "forecast.pt"
META_PATH = OUT_DIR / "forecast_meta.json"

# 时序配置
WINDOW_MINUTES = 30          # 1 个 step = 30 min
HISTORY_STEPS = 24           # 输入 24 个 step (12h)
FORECAST_STEPS = 8           # 输出 8 个 step (4h)
TRAIN_RATIO = 0.8            # 前 80% 训练，后 20% 当 "未来真实值" 对比

# 模型超参
HIDDEN_DIM = 64
NUM_LAYERS = 2
DROPOUT = 0.2
EPOCHS = 50
BATCH_SIZE = 32
LR = 1e-3

SEED = 42


# ============== 模型定义 ==============
class ForecastMLP(nn.Module):
    """时序预测 MLP: 输入 24*2=48 维, 输出 8 维"""

    def __init__(self, in_dim=HISTORY_STEPS * 2, out_dim=FORECAST_STEPS,
                 hidden=HIDDEN_DIM, layers=NUM_LAYERS, dropout=DROPOUT):
        super().__init__()
        blocks = []
        prev = in_dim
        for i in range(layers):
            blocks.append(nn.Linear(prev, hidden))
            blocks.append(nn.ReLU())
            blocks.append(nn.Dropout(dropout))
            prev = hidden
        blocks.append(nn.Linear(prev, out_dim))
        self.net = nn.Sequential(*blocks)

    def forward(self, x):
        return self.net(x)


# ============== 数据预处理 ==============
def load_and_aggregate(csv_path: Path) -> pd.DataFrame:
    """读消费 CSV，按 30min 窗口聚合得到 (timestamp, visitor_count, total_consume)"""
    print(f"[1/4] 读取 {csv_path} ...")
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    print(f"      原始: {len(df):,} 行, {df.columns.tolist()}")

    # 解析时间 (中文列名 "时间")
    df["时间"] = pd.to_datetime(df["时间"])
    df = df.sort_values("时间").reset_index(drop=True)

    # 按 30min 窗口聚合
    df["window_start"] = df["时间"].dt.floor(f"{WINDOW_MINUTES}min")
    agg = df.groupby("window_start").agg(
        visitor_count=("游客ID", "nunique"),
        total_consume=("消费金额", "sum"),
    ).reset_index()
    agg.columns = ["window_start", "visitor_count", "total_consume"]
    print(f"      聚合后: {len(agg):,} 个 30min 窗口")
    print(f"      时间范围: {agg['window_start'].min()} → {agg['window_start'].max()}")

    return agg


def build_supervised(agg: pd.DataFrame):
    """构建监督学习数据集
    X: (N, HISTORY_STEPS*2) - 24 步的 (visitor_count, total_consume)
    Y: (N, FORECAST_STEPS)  - 8 步的 visitor_count (客流量预测)
    """
    print(f"[2/4] 构建监督学习样本 (history={HISTORY_STEPS} step, forecast={FORECAST_STEPS} step) ...")

    visitors = agg["visitor_count"].values.astype(np.float32)
    consumes = agg["total_consume"].values.astype(np.float32)

    X, Y = [], []
    for i in range(len(visitors) - HISTORY_STEPS - FORECAST_STEPS + 1):
        hist_v = visitors[i:i + HISTORY_STEPS]
        hist_c = consumes[i:i + HISTORY_STEPS]
        future_v = visitors[i + HISTORY_STEPS:i + HISTORY_STEPS + FORECAST_STEPS]
        X.append(np.concatenate([hist_v, hist_c]))
        Y.append(future_v)
    X = np.array(X, dtype=np.float32)
    Y = np.array(Y, dtype=np.float32)
    print(f"      X: {X.shape}, Y: {Y.shape}")
    return X, Y


def split_data(X, Y, train_ratio):
    """按时间切分: 前 train_ratio 训练, 后 (1-train_ratio) 当 "未来真实值" """
    n = len(X)
    n_train = int(n * train_ratio)
    X_train, Y_train = X[:n_train], Y[:n_train]
    X_test, Y_test = X[n_train:], Y[n_train:]
    print(f"      训练集: {len(X_train):,}, 测试集: {len(X_test):,}")
    return X_train, Y_train, X_test, Y_test


def normalize(X_train, Y_train, X_test, Y_test):
    """Z-score 标准化 (用训练集 fit)"""
    x_mean = X_train.mean(axis=0)
    x_std = X_train.std(axis=0) + 1e-6
    y_mean = Y_train.mean(axis=0)
    y_std = Y_train.std(axis=0) + 1e-6

    X_train_n = (X_train - x_mean) / x_std
    X_test_n = (X_test - x_mean) / x_std
    Y_train_n = (Y_train - y_mean) / y_std
    return X_train_n, Y_train_n, X_test_n, Y_test, {
        "x_mean": x_mean.tolist(),
        "x_std": x_std.tolist(),
        "y_mean": y_mean.tolist(),
        "y_std": y_std.tolist(),
    }


# ============== 训练 ==============
def train_model(X_train, Y_train, device: str, epochs: int, batch_size: int, lr: float):
    torch, nn, optim, DataLoader, TensorDataset, device = _import_torch(device)
    torch.manual_seed(SEED)
    np.random.seed(SEED)

    print(f"[3/4] 训练 (device={device}, epochs={epochs}, batch={batch_size}, lr={lr}) ...")
    model = ForecastMLP().to(device)
    optimizer = optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()

    X_t = torch.from_numpy(X_train).float()
    Y_t = torch.from_numpy(Y_train).float()
    loader = DataLoader(TensorDataset(X_t, Y_t), batch_size=batch_size, shuffle=True)

    for epoch in range(1, epochs + 1):
        model.train()
        total_loss = 0
        for xb, yb in loader:
            xb, yb = xb.to(device), yb.to(device)
            pred = model(xb)
            loss = loss_fn(pred, yb)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            total_loss += loss.item() * xb.size(0)
        avg = total_loss / len(X_t)
        if epoch % 10 == 0 or epoch == 1:
            print(f"      epoch {epoch:3d}/{epochs}  loss={avg:.4f}")

    return model, torch, nn


# ============== 评估 + 保存 ==============
def evaluate_and_save(model, X_test, Y_test, scaler, meta_extra, device: str):
    torch, _, _, _, _, device = _import_torch(device)
    model.eval()
    with torch.no_grad():
        X_n = (X_test - np.array(scaler["x_mean"])) / np.array(scaler["x_std"])
        pred_n = model(torch.from_numpy(X_n).float().to(device)).cpu().numpy()
        pred = pred_n * np.array(scaler["y_std"]) + np.array(scaler["y_mean"])

    mae = float(np.mean(np.abs(pred - Y_test)))
    rmse = float(np.sqrt(np.mean((pred - Y_test) ** 2)))
    r2 = 1 - float(np.sum((Y_test - pred) ** 2)) / float(np.sum((Y_test - Y_test.mean()) ** 2) + 1e-9)
    print(f"[4/4] 评估: MAE={mae:.2f}  RMSE={rmse:.2f}  R²={r2:.4f}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    torch.save(model.state_dict(), MODEL_PATH)
    meta = {
        "model_type": "ForecastMLP",
        "in_dim": HISTORY_STEPS * 2,
        "out_dim": FORECAST_STEPS,
        "hidden": HIDDEN_DIM,
        "layers": NUM_LAYERS,
        "dropout": DROPOUT,
        "history_steps": HISTORY_STEPS,
        "forecast_steps": FORECAST_STEPS,
        "window_minutes": WINDOW_MINUTES,
        "scaler": scaler,
        "metrics": {"mae": mae, "rmse": rmse, "r2": r2},
        "train_args": meta_extra,
        "trained_at": datetime.now().isoformat(),
    }
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"      模型: {MODEL_PATH}  ({MODEL_PATH.stat().st_size / 1024:.1f} KB)")
    print(f"      元数据: {META_PATH}")
    return mae, rmse, r2


# ============== 验证加载（确保 VM 也能 load） ==============
def verify_load(device: str):
    """模拟 VM 加载流程"""
    torch, _, _, _, _, device = _import_torch(device)
    print("\n[verify] 模拟 VM 加载流程:")
    with open(META_PATH, "r", encoding="utf-8") as f:
        meta = json.load(f)
    model = ForecastMLP(
        in_dim=meta["in_dim"], out_dim=meta["out_dim"],
        hidden=meta["hidden"], layers=meta["layers"], dropout=meta["dropout"],
    ).to(device)
    model.load_state_dict(torch.load(MODEL_PATH, map_location=device))
    model.eval()
    with torch.no_grad():
        dummy = torch.randn(1, meta["in_dim"]).to(device)
        out = model(dummy)
    print(f"      ✓ 模型加载成功, dummy forward out shape = {tuple(out.shape)}")
    print(f"      ✓ 与训练时 out_dim 一致 ({meta['out_dim']})")


# ============== 主程序 ==============
def main():
    parser = argparse.ArgumentParser(description="智能景区时序预测模型训练 (PyTorch)")
    parser.add_argument("--data", type=Path, default=DATA_CSV, help="消费数据 CSV 路径")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR, help="模型输出目录")
    parser.add_argument("--device", type=str, default="cpu", choices=["cpu", "cuda"])
    parser.add_argument("--epochs", type=int, default=EPOCHS)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--lr", type=float, default=LR)
    parser.add_argument("--train-ratio", type=float, default=TRAIN_RATIO)
    args = parser.parse_args()

    if not args.data.exists():
        print(f"错误: 找不到 {args.data}")
        print("请先跑:  python scripts/generate-raw-data.py")
        sys.exit(1)

    print("=" * 60)
    print(" 智能景区大数据平台 - 时序预测模型训练")
    print("=" * 60)

    agg = load_and_aggregate(args.data)
    X, Y = build_supervised(agg)
    X_train, Y_train, X_test, Y_test = split_data(X, Y, args.train_ratio)
    X_train_n, Y_train_n, X_test_n, _, scaler = normalize(X_train, Y_train, X_test, Y_test)

    model, _, _ = train_model(X_train_n, Y_train_n, args.device, args.epochs, args.batch_size, args.lr)
    evaluate_and_save(model, X_test_n, Y_test, scaler, vars(args), args.device)
    verify_load(args.device)

    print("\n=== 训练完成 ===")
    print("部署到 VM:")
    print(f"  scp {MODEL_PATH}  user@vm:/opt/scenic/models/forecast.pt")
    print(f"  scp {META_PATH}  user@vm:/opt/scenic/models/forecast_meta.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
