function DailyReport({
  reports,

  selectedDay,

  onDayChange,

  report,
}) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Daily Consolidated Report</h2>

        <select
          value={selectedDay}
          onChange={(event) => onDayChange(Number(event.target.value))}
        >
          {reports.map((item) => (
            <option key={item.simulated_day} value={item.simulated_day}>
              Day {item.simulated_day}
            </option>
          ))}
        </select>
      </div>

      {!report ? (
        <p className="empty">No report available.</p>
      ) : (
        <>
          <div className="report-summary">
            <span>Patients: {report.patient_count}</span>

            <span>Complete Labs: {report.lab_status_summary.complete}</span>

            <span>Partial: {report.lab_status_summary.partial}</span>

            <span>Missing: {report.lab_status_summary.missing}</span>
          </div>

          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Patient</th>
                  <th>Avg HR</th>
                  <th>Min SpO₂</th>
                  <th>Max Temp</th>
                  <th>Readings</th>
                  <th>Hemoglobin</th>
                  <th>WBC</th>
                  <th>Creatinine</th>
                  <th>Lab Status</th>
                  <th>Concern</th>
                </tr>
              </thead>

              <tbody>
                {report.patients.map((patient) => (
                  <tr key={patient.patient_id}>
                    <td>{patient.patient_id}</td>

                    <td>{patient.avg_heart_rate}</td>

                    <td>{patient.min_spo2}</td>

                    <td>{patient.max_temperature}</td>

                    <td>{patient.reading_count}</td>

                    <td>{patient.hemoglobin ?? "—"}</td>

                    <td>{patient.wbc ?? "—"}</td>

                    <td>{patient.creatinine ?? "—"}</td>

                    <td>
                      <span
                        className={`status status-${patient.lab_status.toLowerCase()}`}
                      >
                        {patient.lab_status}
                      </span>
                    </td>

                    <td>
                      <span
                        className={`concern concern-${patient.concern_level.toLowerCase()}`}
                      >
                        {patient.concern_level}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  );
}

export default DailyReport;
