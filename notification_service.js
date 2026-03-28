// notification_service.js
/**
 * Standardized Notification Service for Police Data Hub
 * Handles OTP delivery via Email and SMS.
 */

async function sendEmailOtp(email, otp) {
    // In a production environment, you would use nodemailer or a service like SendGrid
    console.log(`[NotificationService] Mock Email sent to ${email} with OTP: ${otp}`);
    return Promise.resolve(true);
}

async function sendSmsOtp(mobile, otp) {
    // In a production environment, you would use Twilio or a similar SMS gateway
    console.log(`[NotificationService] Mock SMS sent to ${mobile} with OTP: ${otp}`);
    return Promise.resolve(true);
}

module.exports = {
    sendEmailOtp,
    sendSmsOtp
};
