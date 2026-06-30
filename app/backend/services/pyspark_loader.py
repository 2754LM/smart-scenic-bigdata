"""
PySpark 模型加载器（双轨模式 - 默认关闭）

工作流：
  1. PySpark 训练脚本 (app/jobs/ml/train.py) 在 spark-master 容器内跑
  2. 模型同时保存到 HDFS (/scenic/models/) 和 /shared/models/
  3. backend 启动时尝试从 PYSPARK_MODELS_DIR 加载 PipelineModel
  4. 预测时调 model.transform(spark_df) 拿到 prediction 字段

当前默认路径：demo-backend 镜像只装了 joblib + scikit-learn + numpy，
没有 PySpark/Java，所有预测走 model_service.py 的 sklearn joblib 路径，
毫秒级响应，零启动开销 (~600MB 节省)。

如果想启用双轨模式：
  1. docker/demo-backend/Dockerfile 里加 pyspark + JDK 安装步骤
  2. demo-backend 启动 env 加 USE_PYSPARK_MODELS=true
  3. 第一次预测会触发 PySpark 本地 JVM, 延迟 2-5s

本文件的函数被 main.py 启动逻辑引用, 但默认情况下永不调用
(USE_PYSPARK_MODELS 默认 false).
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

import config

log = logging.getLogger("smart-scenic.pyspark-loader")

_models: Dict[str, Any] = {}
_spark: Any = None
_loaded = False
_load_error: Optional[str] = None


def is_available() -> bool:
    """检查 PySpark 是否可用 + 模型目录是否存在"""
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False
    return Path(config.PYSPARK_MODELS_DIR).exists()


def get_models_dir() -> Path:
    return Path(config.PYSPARK_MODELS_DIR)


def list_available_models() -> List[str]:
    """列出 /shared/models/ 下所有可用模型"""
    d = get_models_dir()
    if not d.exists():
        return []
    return [p.name for p in d.iterdir() if p.is_dir()]


def _get_spark():
    """惰性初始化 SparkSession（只在一个 Spark 进程里创建一个实例）"""
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession
        _spark = SparkSession.builder \
            .appName("SmartScenic-Backend-Load") \
            .config("spark.ui.showConsoleProgress", "false") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def load_all() -> bool:
    """加载所有可用模型到内存。返回是否成功加载至少一个。"""
    global _loaded, _load_error

    if not config.USE_PYSPARK_MODELS:
        log.info("USE_PYSPARK_MODELS=false, skip loading")
        return False

    if _loaded:
        return True

    if not is_available():
        log.info("PySpark not available or %s not exist, fallback to sklearn",
                 config.PYSPARK_MODELS_DIR)
        _load_error = "pyspark_unavailable"
        return False

    from pyspark.ml import PipelineModel

    spark = _get_spark()
    available = list_available_models()
    if not available:
        log.info("No models in %s, fallback to sklearn", config.PYSPARK_MODELS_DIR)
        _load_error = "no_models"
        return False

    loaded_count = 0
    for name in available:
        path = get_models_dir() / name
        try:
            model = PipelineModel.load(str(path))
            _models[name] = model
            log.info("  loaded %s from %s", name, path)
            loaded_count += 1
        except Exception as e:
            log.warning("  failed to load %s: %s", name, e)

    if loaded_count == 0:
        _load_error = "load_failed"
        return False

    _loaded = True
    log.info("PySpark models loaded: %d", loaded_count)
    return True


def is_loaded() -> bool:
    return _loaded


def get_status() -> Dict[str, Any]:
    """返回状态信息（给 /api/predict/_status 用）"""
    return {
        "pyspark_enabled": config.USE_PYSPARK_MODELS,
        "pyspark_available": is_available(),
        "pyspark_loaded": _loaded,
        "models_loaded": list(_models.keys()),
        "models_dir": str(get_models_dir()),
        "load_error": _load_error,
    }


def predict(
    model_name: str,
    feature_cols: List[str],
    feature_values: List[float],
) -> Optional[float]:
    """用 PySpark 模型做单条预测

    Parameters
    ----------
    model_name : str
        模型名（如 "regression_linear" / "classification_rf"）
    feature_cols : list
        特征列名（必须跟训练时一致）
    feature_values : list
        特征值（顺序对应 feature_cols）

    Returns
    -------
    float or None
        预测值；失败返回 None（让 caller fallback 到 sklearn）
    """
    if not _loaded or model_name not in _models:
        return None

    try:
        spark = _get_spark()
        model = _models[model_name]

        # 构造单行 DataFrame
        row_dict = dict(zip(feature_cols, feature_values))
        pdf = pd.DataFrame([row_dict])
        df = spark.createDataFrame(pdf)

        # PipelineModel 会自动跑 assembler + scaler + 模型
        result = model.transform(df).collect()[0]

        # 提取 prediction（不同任务 prediction 列名可能不同）
        if "prediction" in result.asDict():
            return float(result["prediction"])
    except Exception as e:
        log.warning("PySpark predict failed for %s: %s", model_name, e)

    return None


def predict_regression(task: str, features: Dict[str, float]) -> Optional[Dict[str, Any]]:
    """回归预测（自动找最佳模型）

    task: "consumption_amount" / "daily_visitor"
    """
    # 选模型：优先 rf > ridge > lasso > linear（按性能）
    candidates = [
        f"regression_rf",
        f"regression_ridge",
        f"regression_lasso",
        f"regression_linear",
    ]
    for m in candidates:
        if m in _models:
            val = predict(m, list(features.keys()), list(features.values()))
            if val is not None:
                return {"prediction": val, "model": m}
    return None


def predict_classification(features: Dict[str, float]) -> Optional[Dict[str, Any]]:
    """分类预测（高消费游客）"""
    candidates = ["classification_rf", "classification_dt"]
    for m in candidates:
        if m in _models:
            val = predict(m, list(features.keys()), list(features.values()))
            if val is not None:
                label = "高消费" if val > 0.5 else "普通"
                return {"prediction": val, "label": label, "model": m}
    return None