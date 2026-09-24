import {
  useCallback,
  useEffect,
  useState
} from "react";

import {
  getWardOverview,
  getPatients,
  getPatientLatest,
  getPatientHistory,
  getAlerts,
  getReports,
  getDailyReport,
  getHealth
} from "./api";

import MetricCard from "./components/MetricCard";
import PatientMonitor from "./components/PatientMonitor";
import AlertPanel from "./components/AlertPanel";
import DailyReport from "./components/DailyReport";


function App() {

  const [overview, setOverview] =
    useState(null);

  const [patients, setPatients] =
    useState([]);

  const [
    selectedPatient,
    setSelectedPatient
  ] = useState("");

  const [latest, setLatest] =
    useState(null);

  const [history, setHistory] =
    useState([]);

  const [alerts, setAlerts] =
    useState([]);

  const [reports, setReports] =
    useState([]);

  const [
    selectedDay,
    setSelectedDay
  ] = useState("");

  const [report, setReport] =
    useState(null);

  const [error, setError] =
    useState("");

    const [health, setHealth] =
  useState(null);


  const loadWardData =
    useCallback(async () => {

      try {

        const [
          overviewData,
          alertData,
          healthData
        ] = await Promise.all([

          getWardOverview(),

          getAlerts(10),

          getHealth()

        ]);

        setOverview(
          overviewData
        );

        setAlerts(
          alertData.alerts
        );

        setHealth(healthData);

        setError("");

      } catch (err) {

        console.error(err);

        setError(
          "Unable to load live ward data."
        );
      }

    }, []);


  useEffect(() => {

    async function initialize() {

      try {

        const [
          patientData,
          reportData
        ] = await Promise.all([

          getPatients(),

          getReports()

        ]);


        setPatients(
          patientData.patients
        );


        if (
          patientData
            .patients
            .length > 0
        ) {

          setSelectedPatient(
            patientData.patients[0]
          );
        }


        setReports(
          reportData.reports
        );


        if (
          reportData
            .reports
            .length > 0
        ) {

          setSelectedDay(
            reportData
              .reports[0]
              .simulated_day
          );
        }


        setError("");

      } catch (err) {

        console.error(err);

        setError(
          "Unable to initialize dashboard."
        );
      }

    }


    initialize();

    loadWardData();


    const interval =
      setInterval(
        loadWardData,
        5000
      );


    return () =>
      clearInterval(interval);

  }, [loadWardData]);


  useEffect(() => {

    if (!selectedPatient) {
      return;
    }


    async function loadPatient() {

      try {

        const [
          latestData,
          historyData
        ] = await Promise.all([

          getPatientLatest(
            selectedPatient
          ),

          getPatientHistory(
            selectedPatient,
            30
          )

        ]);


        setLatest(
          latestData
        );

        setHistory(
          historyData.readings
        );

      } catch (err) {

        console.error(err);

      }
    }


    loadPatient();


    const interval =
      setInterval(
        loadPatient,
        5000
      );


    return () =>
      clearInterval(interval);

  }, [selectedPatient]);


  useEffect(() => {

    if (!selectedDay) {
      return;
    }


    async function loadReport() {

      try {

        const data =
          await getDailyReport(
            selectedDay
          );

        setReport(data);

      } catch (err) {

        console.error(err);

        setReport(null);

      }
    }


    loadReport();

  }, [selectedDay]);


  return (

    <div className="app">

      <header className="header">

        <div>

          <h1>
            Hospital Monitoring Dashboard
          </h1>

          <p>
            Big Data Patient Vital Signs
            Monitoring Platform
          </p>

        </div>


        <div
          className={
            health?.status === "healthy"
              ? "health-indicator healthy"
              : "health-indicator unhealthy"
          }
        >

          <span className="live-dot" />

          {health?.status === "healthy"
            ? "LIVE"
            : "STALE"}

          {overview && (
            <small>
              Day {overview.simulated_day}
            </small>
          )}

        </div>

      </header>


      {error && (

        <div className="error-banner">
          {error}
        </div>

      )}


      <main>

        <div className="hero-banner">
          <h2>Partnering for Better Health</h2>
          <p>Real-time vital signs and laboratory result monitoring system.</p>
        </div>

        <section className="metrics-grid">

          <MetricCard
            title="Active Patients"
            value={
              overview
                ?.active_patients
            }
            isPrimary={true}
          />

          <MetricCard
            title="Total Readings"
            value={
              overview
                ?.total_readings
            }
          />

          <MetricCard
            title="Abnormal Readings"
            value={
              overview
                ?.abnormal_readings
            }
          />

          <MetricCard
            title="Patients with Alerts"
            value={
              overview
                ?.patients_with_alerts
            }
          />

        </section>


        <PatientMonitor

          patients={patients}

          selectedPatient={
            selectedPatient
          }

          onPatientChange={
            setSelectedPatient
          }

          latest={latest}

          history={history}

        />


        <AlertPanel
          alerts={alerts}
        />


        <DailyReport

          reports={reports}

          selectedDay={
            selectedDay
          }

          onDayChange={
            setSelectedDay
          }

          report={report}

        />

      </main>

    </div>
  );
}


export default App;