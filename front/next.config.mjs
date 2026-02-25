/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'export',
  distDir: 'dist',
  typescript: {
    ignoreBuildErrors: false,
  },
  images: {
    unoptimized: true,
  },
  // Enable trailing slashes so static export generates dir/index.html files
  // (required for ServeDir fallback to work correctly with SPA routing)
  trailingSlash: true,
}

export default nextConfig
