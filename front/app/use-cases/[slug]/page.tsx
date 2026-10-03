import { RedirectNotice } from "@/components/redirect-notice"
import { RETIRED_USE_CASE_SLUGS, USE_CASE_REDIRECT, USE_CASE_REDIRECT_LABEL, redirectMetadata } from "../redirect"

export const metadata = redirectMetadata

export default function UseCasePage() {
  return <RedirectNotice to={USE_CASE_REDIRECT} label={USE_CASE_REDIRECT_LABEL} />
}

export function generateStaticParams() {
  return RETIRED_USE_CASE_SLUGS.map((slug) => ({ slug }))
}
