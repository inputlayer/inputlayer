import { RedirectNotice } from "@/components/redirect-notice"
import { USE_CASE_REDIRECT, USE_CASE_REDIRECT_LABEL, redirectMetadata } from "./redirect"

export const metadata = redirectMetadata

export default function UseCasesPage() {
  return <RedirectNotice to={USE_CASE_REDIRECT} label={USE_CASE_REDIRECT_LABEL} />
}
