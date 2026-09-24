function getReasons(alert) {

  const reasons = [];

  if (alert.high_heart_rate) {
    reasons.push("High heart rate");
  }

  if (alert.low_spo2) {
    reasons.push("Low SpO₂");
  }

  if (alert.high_temperature) {
    reasons.push("High temperature");
  }

  return reasons.join(", ");
}


function AlertPanel({ alerts }) {

  return (

    <section className="panel">

      <div className="panel-header">

        <h2>Recent Patient Alerts</h2>

        <span className="alert-count">
          {alerts.length}
        </span>

      </div>


      {alerts.length === 0 ? (

        <p className="empty">
          No recent abnormal readings.
        </p>

      ) : (

        <div className="alert-list">

          {alerts.map(
            (alert, index) => (

              <div
                className="alert-item"
                key={
                  `${alert.patient_id}-${alert.event_time}-${index}`
                }
              >

                <div>

                  <strong>
                    {alert.patient_id}
                  </strong>

                  <p>
                    {getReasons(alert)}
                  </p>

                </div>


                <div className="alert-values">

                  HR {alert.heart_rate}

                  {" | "}

                  SpO₂ {alert.spo2}%

                  {" | "}

                  {alert.temperature}°C

                </div>

              </div>

            )
          )}

        </div>

      )}

    </section>
  );
}


export default AlertPanel;