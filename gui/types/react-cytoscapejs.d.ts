declare module "react-cytoscapejs" {
  import type cytoscape from "cytoscape"
  import type { ComponentType } from "react"

  interface CytoscapeComponentProps {
    elements: cytoscape.ElementDefinition[]
    stylesheet?: cytoscape.Stylesheet[]
    layout?: cytoscape.LayoutOptions
    cy?: (cy: cytoscape.Core) => void
    style?: React.CSSProperties
    className?: string
  }

  const CytoscapeComponent: ComponentType<CytoscapeComponentProps>
  export default CytoscapeComponent
}
