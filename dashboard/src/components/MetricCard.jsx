function MetricCard({
  title,
  value
}) {

  return (
    <div className="metric-card">

      <span className="metric-title">
        {title}
      </span>

      <strong className="metric-value">
        {value ?? 0}
      </strong>

    </div>
  );
}

export default MetricCard;