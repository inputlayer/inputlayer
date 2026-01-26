import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'export',
  distDir: 'dist',
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    unoptimized: true,
  },
  // Disable trailing slashes for cleaner URLs
  trailingSlash: false,
  // Transpile local packages
  transpilePackages: ['@inputlayer/api-client'],
  // Configure webpack to resolve the local package
  webpack: (config) => {
    config.resolve.alias['@inputlayer/api-client'] = path.resolve(
      __dirname,
      '../packages/api-client/dist/index.mjs'
    );
    return config;
  },
}

export default nextConfig
