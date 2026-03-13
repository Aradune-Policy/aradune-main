/**
 * Chart Export Utility
 * Exports Recharts SVG charts to PNG or SVG files.
 * No external dependencies — uses native browser APIs.
 */

/**
 * Find the Recharts SVG element inside a container ref.
 * Recharts renders an <svg class="recharts-surface"> inside a wrapper div.
 */
function findChartSVG(container: HTMLElement): SVGSVGElement | null {
  return container.querySelector("svg.recharts-surface") || container.querySelector("svg");
}

/**
 * Serialize an SVG element to a string with proper XML declaration.
 */
function serializeSVG(svg: SVGSVGElement): string {
  const clone = svg.cloneNode(true) as SVGSVGElement;
  // Ensure width/height attributes are set
  if (!clone.getAttribute("width")) {
    clone.setAttribute("width", String(svg.getBoundingClientRect().width));
  }
  if (!clone.getAttribute("height")) {
    clone.setAttribute("height", String(svg.getBoundingClientRect().height));
  }
  // Add white background
  const bg = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  bg.setAttribute("width", "100%");
  bg.setAttribute("height", "100%");
  bg.setAttribute("fill", "white");
  clone.insertBefore(bg, clone.firstChild);

  const serializer = new XMLSerializer();
  return '<?xml version="1.0" encoding="UTF-8"?>\n' + serializer.serializeToString(clone);
}

/**
 * Download an SVG chart as an .svg file.
 */
export function downloadChartSVG(container: HTMLElement, filename?: string): boolean {
  const svg = findChartSVG(container);
  if (!svg) return false;

  const svgString = serializeSVG(svg);
  const blob = new Blob([svgString], { type: "image/svg+xml;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename ?? `aradune-chart-${new Date().toISOString().slice(0, 10)}.svg`;
  a.click();
  URL.revokeObjectURL(url);
  return true;
}

/**
 * Convert an SVG element to a PNG blob via Canvas.
 * Returns a Promise<Blob> for the PNG data.
 */
function svgToPNG(svg: SVGSVGElement, scale = 2): Promise<Blob> {
  return new Promise((resolve, reject) => {
    const svgString = serializeSVG(svg);
    const width = svg.getBoundingClientRect().width * scale;
    const height = svg.getBoundingClientRect().height * scale;

    const canvas = document.createElement("canvas");
    canvas.width = width;
    canvas.height = height;
    const ctx = canvas.getContext("2d");
    if (!ctx) return reject(new Error("Canvas 2D context not available"));

    const img = new Image();
    const svgBlob = new Blob([svgString], { type: "image/svg+xml;charset=utf-8" });
    const url = URL.createObjectURL(svgBlob);

    img.onload = () => {
      ctx.drawImage(img, 0, 0, width, height);
      URL.revokeObjectURL(url);
      canvas.toBlob(
        (blob) => {
          if (blob) resolve(blob);
          else reject(new Error("Canvas toBlob failed"));
        },
        "image/png"
      );
    };

    img.onerror = () => {
      URL.revokeObjectURL(url);
      reject(new Error("SVG image load failed"));
    };

    img.src = url;
  });
}

/**
 * Download a chart as a PNG file.
 * Uses 2x scale for retina-quality output.
 */
export async function downloadChartPNG(
  container: HTMLElement,
  filename?: string
): Promise<boolean> {
  const svg = findChartSVG(container);
  if (!svg) return false;

  try {
    const blob = await svgToPNG(svg, 2);
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename ?? `aradune-chart-${new Date().toISOString().slice(0, 10)}.png`;
    a.click();
    URL.revokeObjectURL(url);
    return true;
  } catch {
    return false;
  }
}

/**
 * Get a PNG Blob from a chart container (for embedding in PDFs, etc.).
 */
export async function chartToPNGBlob(
  container: HTMLElement,
  scale = 2
): Promise<Blob | null> {
  const svg = findChartSVG(container);
  if (!svg) return null;
  try {
    return await svgToPNG(svg, scale);
  } catch {
    return null;
  }
}
