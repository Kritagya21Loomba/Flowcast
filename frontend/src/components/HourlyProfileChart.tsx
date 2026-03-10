import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'

interface Props {
  profile: number[]
}

export default function HourlyProfileChart({ profile }: Props) {
  const data = profile.map((value, hour) => ({ hour: `${hour}`, value }))

  return (
    <ResponsiveContainer width="100%" height={150}>
      <BarChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
        <XAxis
          dataKey="hour"
          tick={{ fill: '#888', fontSize: 9 }}
          interval={2}
        />
        <YAxis tick={{ fill: '#888', fontSize: 10 }} width={50} tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`} />
        <Tooltip
          contentStyle={{ background: '#1a1d23', border: '1px solid #2a2d35', fontSize: '0.8rem' }}
          labelFormatter={(h) => `Hour ${h}:00`}
          formatter={(value) => [Number(value).toLocaleString(), 'Volume']}
        />
        <Bar dataKey="value" fill="#60a5fa" radius={[2, 2, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}
