// /api/create-checkout.js — Stripe Checkout Session creation
// POST { plan: "individual" | "organization" }
// Returns { url } for redirect to Stripe Checkout

import Stripe from "stripe";

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

const PRICE_IDS = {
  individual: process.env.STRIPE_PRICE_ID_INDIVIDUAL,
  organization: process.env.STRIPE_PRICE_ID_ORG,
};

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const { plan } = req.body || {};
  const priceId = PRICE_IDS[plan];

  if (!priceId) {
    return res.status(400).json({ error: "Invalid plan. Use 'individual' or 'organization'." });
  }

  try {
    const origin = `https://${req.headers.host}`;

    const session = await stripe.checkout.sessions.create({
      mode: "subscription",
      line_items: [{ price: priceId, quantity: 1 }],
      success_url: `${origin}/#/analyst?session={CHECKOUT_SESSION_ID}`,
      cancel_url: `${origin}/#/pricing`,
    });

    return res.status(200).json({ url: session.url });
  } catch (error) {
    console.error("Stripe checkout error:", error);
    return res.status(500).json({ error: "Failed to create checkout session" });
  }
}
