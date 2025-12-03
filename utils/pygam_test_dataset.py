"""
Simple pygam test dataset generator and example fit.

Run:
    python utils/pygam_test_dataset.py

This will:
- generate a small synthetic dataset
- save it to `utils/pygam_test_data.csv`
- fit a simple `LinearGAM` (if `pygam` is installed) and print a short summary

The dataset structure:
- If `n_features==1`: X is a single column in [0, 1], y = sin(2*pi*X*3) + 0.5*X + noise
- If `n_features>1`: additional features are simple transforms of the base X

Designed for quick experimentation with pygam.
"""

from __future__ import annotations
import numpy as np
import pandas as pd
import os

try:
    from pygam import LinearGAM, s
    _HAS_PYGAM = True
except Exception:
    _HAS_PYGAM = False


def generate_pygam_test_data(n_samples: int = 300, n_features: int = 1, noise: float = 0.3, random_state: int | None = 0):
    """Generate a small synthetic dataset for testing pygam.

    Returns
    -------
    X : ndarray, shape (n_samples, n_features)
    y : ndarray, shape (n_samples,)
    """
    rng = np.random.default_rng(random_state)
    x0 = rng.random(n_samples)

    # base nonlinear signal
    y = np.sin(2 * np.pi * 3 * x0) + 0.5 * x0

    if n_features == 1:
        X = x0.reshape(-1, 1)
    else:
        # make additional features as transforms of x0
        features = [x0]
        for k in range(1, n_features):
            if k % 3 == 1:
                features.append(np.sqrt(x0 + 1e-6))
            elif k % 3 == 2:
                features.append(x0 ** 2)
            else:
                features.append(rng.normal(scale=0.1, size=n_samples))
        X = np.vstack(features).T

    # add gaussian noise
    y = y + rng.normal(scale=noise, size=n_samples)

    return X, y


def save_dataset(X: np.ndarray, y: np.ndarray, path: str):
    """Save dataset to CSV with header columns `x0`, `x1`, ..., `y`."""
    df = pd.DataFrame(X, columns=[f"x{i}" for i in range(X.shape[1])])
    df["y"] = y
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


def example_fit(X: np.ndarray, y: np.ndarray, n_splines: int = 20):
    """Fit a simple GAM on the first column and print a short summary.

    If `pygam` is not installed, explain how to install it.
    """
    if not _HAS_PYGAM:
        print("pygam is not installed. Install with: pip install pygam")
        return

    gam = LinearGAM(s(0, n_splines=n_splines)).fit(X[:, 0], y)
    print(gam.summary())
    return gam


if __name__ == "__main__":
    # generate and save
    X, y = generate_pygam_test_data(n_samples=300, n_features=1, noise=0.25, random_state=42)
    out_csv = os.path.join(os.path.dirname(__file__), "pygam_test_data.csv")
    save_dataset(X, y, out_csv)
    print(f"Saved test dataset to: {out_csv}")

    # try a quick example fit
    example_fit(X, y)
