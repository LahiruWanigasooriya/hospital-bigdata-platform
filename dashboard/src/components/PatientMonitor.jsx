import VitalChart from "./VitalChart";


function PatientMonitor({

  patients,

  selectedPatient,

  onPatientChange,

  latest,

  history

}) {

  return (

    <section className="panel">

      <div className="panel-header">

        <h2>Patient Monitor</h2>

        <select
          value={selectedPatient}
          onChange={(event) =>
            onPatientChange(
              event.target.value
            )
          }
        >

          {patients.map(
            (patient) => (

              <option
                key={patient}
                value={patient}
              >
                {patient}
              </option>

            )
          )}

        </select>

      </div>


      {!latest ? (

        <p className="empty">
          No patient data available.
        </p>

      ) : (

        <>

          <div className="vital-grid">

            <div className="vital-box">
              <span>Heart Rate</span>
              <strong>
                {latest.heart_rate} bpm
              </strong>
            </div>


            <div className="vital-box">
              <span>SpO₂</span>
              <strong>
                {latest.spo2}%
              </strong>
            </div>


            <div className="vital-box">
              <span>Blood Pressure</span>
              <strong>
                {latest.systolic_bp}/
                {latest.diastolic_bp}
              </strong>
            </div>


            <div className="vital-box">
              <span>Temperature</span>
              <strong>
                {latest.temperature} °C
              </strong>
            </div>

          </div>


          {latest.has_alert && (

            <div className="patient-warning">
              Abnormal vital reading detected
            </div>

          )}


          <VitalChart
            readings={history}
          />

        </>

      )}

    </section>
  );
}


export default PatientMonitor;