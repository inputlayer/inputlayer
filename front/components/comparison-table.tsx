import { CheckCircle, XCircle, Minus, RotateCcw } from "lucide-react"

type CellValue = "native" | "plugin" | "partial" | "manual" | "recompute" | "n/a" | "none"

const cellConfig: Record<CellValue, { icon: React.ReactNode; label?: string }> = {
  native: { icon: <CheckCircle className="h-4 w-4 text-emerald-500" /> },
  plugin: { icon: <CheckCircle className="h-4 w-4 text-yellow-500" />, label: "plugin" },
  partial: { icon: <Minus className="h-4 w-4 text-yellow-500" />, label: "partial" },
  manual: { icon: <Minus className="h-4 w-4 text-yellow-500" />, label: "manual" },
  recompute: { icon: <RotateCcw className="h-4 w-4 text-yellow-500" />, label: "recompute" },
  "n/a": { icon: <Minus className="h-4 w-4 text-muted-foreground/40" />, label: "n/a" },
  none: { icon: <XCircle className="h-4 w-4 text-muted-foreground/40" /> },
}

function ComparisonCell({ value }: { value: CellValue }) {
  const config = cellConfig[value] ?? cellConfig.none
  return (
    <span className="inline-flex flex-col items-center gap-0.5">
      {config.icon}
      {config.label && <span className="text-[10px] text-muted-foreground">{config.label}</span>}
    </span>
  )
}

interface ComparisonRow {
  capability: string
  values: Record<string, CellValue>
}

interface ComparisonTableProps {
  columns: string[]
  highlightColumn?: string
  rows: ComparisonRow[]
}

export function ComparisonTable({ columns, highlightColumn, rows }: ComparisonTableProps) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr className="border-b border-border">
            <th className="text-left py-3 px-4 font-semibold">Capability</th>
            {columns.map((col) => (
              <th
                key={col}
                className={`text-center py-3 px-4 font-semibold ${
                  col === highlightColumn ? "text-primary" : "text-muted-foreground"
                }`}
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.capability} className="border-b border-border/50">
              <td className="py-3 px-4">{row.capability}</td>
              {columns.map((col) => (
                <td key={col} className="py-3 px-4 text-center">
                  <span className="inline-flex justify-center w-full">
                    <ComparisonCell value={row.values[col] || "none"} />
                  </span>
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
