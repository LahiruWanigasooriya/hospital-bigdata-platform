import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer
} from "recharts";


function VitalChart({ readings }) {

  const data = readings.map(
    (reading) => ({

      ...reading,

      time: new Date(
        reading.event_time
      ).toLocaleTimeString()

    })
  );


  if (data.length === 0) {

    return (
      <p className="empty">
        No history available.
      </p>
    );
  }


  return (

    <div className="chart-container">

      <h3>Recent Vital Trends</h3>

      <ResponsiveContainer
        width="100%"
        height={300}
      >

        <LineChart data={data}>

          <XAxis
            dataKey="time"
            minTickGap={30}
          />

          <YAxis />

          <Tooltip />

          <Legend />

          <Line
            type="monotone"
            dataKey="heart_rate"
            name="Heart Rate"
            dot={false}
          />

          <Line
            type="monotone"
            dataKey="spo2"
            name="SpO₂"
            dot={false}
          />

          <Line
            type="monotone"
            dataKey="temperature"
            name="Temperature"
            dot={false}
          />

        </LineChart>

      </ResponsiveContainer>

    </div>
  );
}


export default VitalChart;