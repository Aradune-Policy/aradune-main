// /api/activate.js — Token claim after Stripe payment
// GET /api/activate?session=cs_xxx
// Verifies paid session, generates signed token, returns { token, email }

import Stripe from "stripe";
import { createHmac } from "crypto";

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

function base64url(buf) {
  return buf.toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function signToken(customerId) {
  const payload = base64url(Buffer.from(JSON.stringify({ cid: customerId, iat: Math.floor(Date.now() / 1000) })));
  const sig = createHmac("sha256", process.env.JWT_SECRET).update(payload).digest();
  return `${payload}.${base64url(sig)}`;
}

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "GET") return res.status(405).json({ error: "Method not allowed" });

  const sessionId = req.query.session;
  if (!sessionId || !sessionId.startsWith("cs_")) {
    return res.status(400).json({ error: "Invalid session ID" });
  }

  try {
    const session = await stripe.checkout.sessions.retrieve(sessionId);

    if (session.payment_status !== "paid") {
      return res.status(402).json({ error: "Payment not completed" });
    }

    const customerId = session.customer;
    const token = signToken(customerId);

    return res.status(200).json({
      token,
      email: session.customer_details?.email || null,
    });
  } catch (error) {
    console.error("Activation error:", error);
    if (error.type === "StripeInvalidRequestError") {
      return res.status(400).json({ error: "Invalid session" });
    }
    return res.status(500).json({ error: "Activation failed" });
  }
}
