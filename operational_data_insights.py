import pandas as pd
from typing import Optional, Dict, Any
from loguru import logger

logger.configure(
    handlers=[dict(
        sink=sys.stderr,
        format="[{level}] {message}",
        level="INFO"
    )]
)

class OperationalDataInsights:
    def __init__(self):
        self.operational_data: Optional[pd.DataFrame] = None
        
    def update_data(self, data: pd.DataFrame) -> None:
        """Updates the internal operational data with new insights."""
        self.operational_data = data
        logger.info("Operational data updated successfully.")
        
    def _compute OperationalMetrics(
        self,
        key_metric: str,
        window_size: int
    ) -> Dict[str, Any]:
        """
        Computes operational metrics for a specified metric and window size.
        Args:
            key_metric: The metric to analyze (e.g., 'revenue', 'margin').
            window_size: Number of periods to consider.
        Returns:
            A dictionary containing the computed operational metrics.
        Raises:
            ValueError: If data is not available for the specified metric or window.
        """
        if self.operational_data is None:
            raise ValueError("Operational data not initialized.")
            
        recent_periods = self.operational_data[-window_size:]
        
        if len(recent_periods) == 0:
            raise ValueError("No data available for the specified window.")
            
        metrics = {
            'average_change': recent_periods[key_metric].mean(),
            'max_value': recent_periods[key_metric].max(),
            'min_value': recent_periods[key_metric].min()
        }
        
        return metrics
    
    def analyze_operational_metrics(
        self,
        key_metric: str = 're