// Two timelines: a batch agent that only sees changes when its next run fires,
// and a live agent that is told about each change as it lands.

const changes = [160, 330, 470, 610, 790]
const runs = [
  { x: 50, width: 70 },
  { x: 520, width: 70 },
  { x: 940, width: 60 },
]
const changeLabels = [
  { x: 160, text: "order slips" },
  { x: 300, text: "approval revoked" },
  { x: 610, text: "delay clears" },
  { x: 760, text: "stock runs out" },
]

export function BatchVsLiveDiagram() {
  return (
    <svg
      viewBox="0 0 1000 300"
      role="img"
      aria-labelledby="batch-vs-live-title"
      className="block h-auto w-full min-w-[640px] text-foreground"
    >
      <title id="batch-vs-live-title">
        A batch agent is blind between runs; a live agent is told about each change
      </title>
      <defs>
        <marker id="bvs-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
          <path d="M0 0L10 5L0 10z" fill="var(--primary)" />
        </marker>
      </defs>

      <g fill="currentColor" fontSize="15" fontWeight="700">
        <text x="0" y="24">Batch agent</text>
        <text x="0" y="168">Live agent</text>
      </g>

      <g stroke="currentColor" strokeOpacity="0.35" strokeWidth="2">
        <line x1="0" y1="80" x2="1000" y2="80" />
        <line x1="0" y1="226" x2="1000" y2="226" />
      </g>

      {/* Batch lane: changes land between runs and go unseen */}
      <g fill="var(--destructive)">
        {changes.map((cx) => (
          <circle key={cx} cx={cx} cy="80" r="5" />
        ))}
      </g>
      <g fill="var(--primary)">
        {runs.map((r) => (
          <rect key={r.x} x={r.x} y="62" width={r.width} height="36" rx="8" />
        ))}
      </g>
      <g fill="var(--primary-foreground)" fontFamily="var(--font-geist-mono), monospace" fontSize="12">
        {runs.map((r) => (
          <text key={r.x} x={r.x + 12} y="85">
            run
          </text>
        ))}
      </g>
      <g fill="currentColor" fillOpacity="0.7" fontSize="12.5">
        {changeLabels.map((l) => (
          <text key={l.text} x={l.x} y="118">
            {l.text}
          </text>
        ))}
        <text x="190" y="52">blind until next run</text>
        <text x="660" y="52">blind until next run</text>
      </g>

      {/* Live lane: every change reaches the agent */}
      <g fill="var(--destructive)">
        {changes.map((cx) => (
          <circle key={cx} cx={cx} cy="226" r="5" />
        ))}
      </g>
      <g stroke="var(--primary)" strokeWidth="2.5">
        {changes.map((x) => (
          <line key={x} x1={x} y1="226" x2={x} y2="262" markerEnd="url(#bvs-arrow)" />
        ))}
      </g>
      <text x="160" y="290" fill="currentColor" fillOpacity="0.7" fontSize="12.5">
        the agent is told what changed, including the conclusions that stopped being true
      </text>
    </svg>
  )
}
