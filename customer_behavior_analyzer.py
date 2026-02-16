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

class CustomerBehaviorAnalyzer:
    def __init__(self):
        self.behavior_data: Optional[pd.DataFrame] = None
        
    def update_data(self, data: pd.DataFrame) -> None:
        """Updates the internal behavior data with new customer insights."""
        self.behavior_data = data
        logger.info("Customer behavior data updated successfully.")
        
    def _compute_behavior_metrics(
        self,
        target_segment: str,
        time_window: int
    ) -> Dict[str, Any]:
        """
        Computes customer behavior metrics for a specified segment and time window.
        Args:
            target_segment: The customer segment to analyze (e.g., 'loyal', 'new').
            time_window: Number of days to consider.
        Returns:
            A dictionary containing the computed metrics.
        Raises:
            ValueError: If data is not available for the specified segment or window.
        """
        if self.behavior_data is None:
            raise ValueError("Behavior data not initialized.")
            
        recent_period = pd.to_datetime('now') - pd.Timedelta(days=time_window)
        
        filtered_segment = self.behavior_data[
            (self.behavior_data['segment'] == target_segment) &
            (self.behavior_data['timestamp'] >= recent_period)
        ]
        
        if len(filtered_segment) == 0:
            raise ValueError("No data available for the specified segment and window.")
            
        metrics = {
            'purchase_frequency': filtered_segment['purchase_count'].mean(),
            'average_spending': filtered_segment['revenue_per_purchase'].mean()
        }
        return metrics
    
    def analyze_behavior(
        self,
        target_segment: str = 'loyal',
        time_window: int = 30
    ) -> Dict[str, Any]:
        """
        Analyzes customer behavior for a specified segment and time window.
        Args:
            target_segment: The customer segment to analyze (e.g., 'loyal', 'new').
            time_window: Number of days to consider.
        Returns:
            A dictionary containing the computed behavior metrics.
        """
        try:
            behavior_metrics = self._compute_behavior_metrics(target_segment, time_window)
            logger.info("Customer behavior analysis completed successfully.")
            return behavior_metrics
        except Exception as e:
            logger.error(f"Error in customer behavior analysis: {str(e)}")
            raise