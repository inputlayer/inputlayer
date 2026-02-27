import { CheckCircle, XCircle, Minus } from "lucide-react"

type CellValue = "native" | "plugin" | "partial" | "none"

function ComparisonIcon({ value }: { value: CellValue }) {
  switch (value) {
    case "native":
      return <CheckCircle className="h-4 w-4 text-emerald-500" />
    case "plugin":
      return <CheckCircle className="h-4 w-4 text-yellow-500" />
    case "partial":
      return <Minus className="h-4 w-4 text-yellow-500" />
    case "none":
      return <XCircle className="h-4 w-4 text-muted-foreground/40" />
  }
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
                    <ComparisonIcon value={row.values[col] || "none"} />
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
