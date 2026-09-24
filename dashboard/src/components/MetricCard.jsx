function MetricCard({
  title,
  value,
  isPrimary
}) {

  return (
    <div className={`metric-card ${isPrimary ? "primary" : ""}`}>

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