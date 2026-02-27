import { customerStories } from "@/lib/content-bundle"
import { CustomersIndexClient } from "./customers-index-client"

export default function CustomersPage() {
  return <CustomersIndexClient stories={customerStories} />
}
